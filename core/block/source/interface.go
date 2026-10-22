package source

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"

	"github.com/anyproto/anytype-heart/core/block/editor/state"
	"github.com/anyproto/anytype-heart/core/block/editor/storestate"
	"github.com/anyproto/anytype-heart/core/domain"
	"github.com/anyproto/anytype-heart/pb"
	"github.com/anyproto/anytype-heart/pkg/lib/core/smartblock"
	"github.com/anyproto/anytype-heart/space/spacecore/typeprovider"
)

var (
	ErrReadOnly          = errors.New("object is read only")
	ErrBigChangeSize     = errors.New("change size is above the limit")
	ErrUnknownDataFormat = fmt.Errorf("unknown data format: you may need to upgrade anytype in order to open this page")
)

type PushChangeHook func(params PushChangeParams) (id string, err error)

type PushStoreChangeParams struct {
	State   *storestate.StoreState
	Changes []*pb.StoreChangeContent
	Time    time.Time // used to derive the lastModifiedDate; Default is time.Now()
}

type ObjectTreeProvider interface {
	Tree() objecttree.ObjectTree
}

type Space interface {
	Id() string
	TreeBuilder() objecttreebuilder.TreeBuilder
	GetRelationIdByKey(ctx context.Context, key domain.RelationKey) (id string, err error)
	GetTypeIdByKey(ctx context.Context, key domain.TypeKey) (id string, err error)
	DeriveObjectID(ctx context.Context, uniqueKey domain.UniqueKey) (id string, err error)
	StoredIds() []string
	IsPersonal() bool
}

type Service interface {
	NewSource(ctx context.Context, space Space, id string, buildOptions BuildOptions) (source Source, err error)
	RegisterStaticSource(s Source) error
	NewStaticSource(params StaticSourceParams) SourceWithType

	DetailsFromIdBasedSource(id domain.FullID) (*domain.Details, error)
	IDsListerBySmartblockType(space Space, blockType smartblock.SmartBlockType) (IDsLister, error)
	app.Component
}

type ReadStoreTreeHook interface {
	BeforeIteration(ot objecttree.ObjectTree)
	OnIteration(ot objecttree.ObjectTree, change *objecttree.Change)
	AfterDiffManagersInit(ctx context.Context) error
}

type ReadStoreDocParams struct {
	OnUpdateHook      func()
	ReadStoreTreeHook ReadStoreTreeHook
}

type Store interface {
	Source
	ReadStoreDoc(ctx context.Context, stateStore *storestate.StoreState, params ReadStoreDocParams) (err error)
	PushStoreChange(ctx context.Context, params PushStoreChangeParams) (changeId string, err error)
	SetPushChangeHook(onPushChange PushChangeHook)

	// AclList exposes the underlying space ACL so handlers can resolve permissions
	// (e.g. ChatHandler.BeforeDelete needs to know if the change author was an admin
	// at the change's AclHeadId).
	AclList() list.AclList

	// RegisterDiffManager sets a hook that will be called when a change is removed (marked as read) from the diff manager
	// must be called before ReadStoreDoc.
	//
	// If a head is marked as read in the diff manager, all earlier heads for that branch marked as read as well
	RegisterDiffManager(name string, onRemoveHook func(removed []string))
	// MarkSeenHeads marks heads as seen in a diff manager. Then the diff manager will call a hook from SetDiffManagerOnRemoveHook
	MarkSeenHeads(ctx context.Context, name string, heads []string) error
	// StoreSeenHeads persists current seen heads in any-store
	StoreSeenHeads(ctx context.Context, name string) error
	// InitDiffManager initializes a diff manager with specified seen heads
	InitDiffManager(ctx context.Context, name string, seenHeads []string) error
}

type PushChangeParams struct {
	State             *state.State
	Changes           []*pb.ChangeContent
	FileChangedHashes []string
	Time              time.Time // used to derive the lastModifiedDate; Default is time.Now()
	DoSnapshot        bool
	ChangeType        domain.ChangeType
	// IntegrationName stamps the pushed change as authored via the API
	// integration with this RAW app name (pb.Change.IntegrationName;
	// APIV2_OBJECT_DELETE.md §5/§11.4 — never normalized, compared exactly).
	// Filled per-apply from the state that produced the change — today only
	// the creation apply fills it; attribution later widens the fill sites
	// with zero wire change.
	IntegrationName string
}

type IDsLister interface {
	ListIds() ([]string, error)
}

type Source interface {
	Id() string
	SpaceID() string
	Type() smartblock.SmartBlockType
	Heads() []string
	GetFileKeysSnapshot() []*pb.ChangeFileKeys
	ReadOnly() bool
	ReadDoc(ctx context.Context, receiver ChangeReceiver, empty bool) (doc state.Doc, err error)
	PushChange(params PushChangeParams) (id string, err error)
	Close() (err error)
	GetCreationInfo() (creatorObjectId string, createdDate int64, err error)
}

type ChangeReceiver interface {
	StateAppend(func(d state.Doc) (s *state.State, changes []*pb.ChangeContent, err error)) error
	StateRebuild(d state.Doc) (err error)
}

type SourceWithType interface {
	Source
	IDsLister
}

type BuildOptions struct {
	DisableRemoteLoad bool
	Listener          updatelistener.UpdateListener
}

func (b *BuildOptions) BuildTreeOpts() objecttreebuilder.BuildTreeOpts {
	return objecttreebuilder.BuildTreeOpts{
		Listener: b.Listener,
		TreeBuilder: func(treeStorage objecttree.Storage, aclList list.AclList) (objecttree.ObjectTree, error) {
			// 明文全量同步 fork：ACL 无读密钥，跳过读密钥校验与按密钥过滤
			// （签名/权限/ACL 顺序校验仍在，见 verifiableTreeDeps）。
			// KeyFilterable 变体会把所有无密钥 change 当不可读过滤掉，
			// 在本分叉里等于丢弃全部同步内容，故不可用。
			ot, err := objecttree.BuildObjectTree(treeStorage, aclList)
			if err != nil {
				return nil, err
			}
			sbt, _, err := typeprovider.GetTypeAndKeyFromRoot(ot.Header())
			if err != nil {
				return nil, err
			}
			if sbt.IsStoreBacked() {
				ot.SetFlusher(objecttree.MarkNewChangeFlusher())
			}
			return ot, nil
		},
		TreeValidator: validateRawTreeNoReadKey,
	}
}

// validateRawTreeNoReadKey mirrors any-sync's ValidateFilterRawTree but
// without the read-permission gate and without key filtering: in this fork
// ACLs carry no read keys, so HadReadPermissions is always false and the
// key-filtering builder would discard every change. Signatures, write
// permissions and ACL ordering are still validated by the (false, false)
// validator inside BuildEmptyDataObjectTree.
func validateRawTreeNoReadKey(payload treestorage.TreeStorageCreatePayload, storageCreator objecttree.TreeStorageCreator, aclList list.AclList) (objecttree.ObjectTree, error) {
	ctx := context.Background()
	treeStorage, err := storageCreator.CreateStorageWithDeferredCreation(ctx, treestorage.TreeStorageCreatePayload{
		RootRawChange: payload.RootRawChange,
		Heads:         []string{payload.RootRawChange.Id},
	})
	if err != nil {
		return nil, err
	}
	tree, err := objecttree.BuildEmptyDataObjectTree(treeStorage, aclList)
	if err != nil {
		return nil, err
	}
	tree.Lock()
	defer tree.Unlock()
	_, err = tree.AddRawChanges(context.Background(), objecttree.RawChangesPayload{
		NewHeads:   payload.Heads,
		RawChanges: payload.Changes,
	})
	if err != nil {
		return nil, err
	}
	if objecttree.IsEmptyTree(tree) {
		return nil, objecttree.ErrNoChangeInTree
	}
	return tree, nil
}

type StaticSourceParams struct {
	Id        domain.FullID
	SbType    smartblock.SmartBlockType
	State     *state.State
	CreatorId string
}
