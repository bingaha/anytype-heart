package spacecore

// 明文全量同步的端到端验证（树层）：同账号两节点 A/B，A 建无密钥空间并写入
// 一段纯文字，B 用对 heads 补缺拿到同样的明文；同步存储中的新变更载荷为
// 明文且 ReadKeyId 为空。Ed25519 变更签名在写入与摄入两侧都照常校验。

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/require"
)

const plaintextTestText = "明文同步验证：A 写一段文字，B 应读到同样明文。"

type plaintextSide struct {
	aclList list.AclList
	tree    objecttree.ObjectTree
	// aclRoot 是 ACL 根原始记录；B 建链时复用它（RecordsBefore/After 均不含根）。
	aclRoot *consensusproto.RawRecordWithId
}

func newPlaintextStore(t *testing.T, name string) anystore.DB {
	t.Helper()
	db, err := anystore.Open(context.Background(), filepath.Join(t.TempDir(), name), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

// 建链：与 service.Create 一致的无密钥空间 ACL（ReadKey 为 nil，根记录不带
// 加密密钥块）；B 复用 A 的 ACL 原始记录重建，模拟同步拉取。
func newKeylessAcl(t *testing.T, keys *accountdata.AccountKeys, records []*consensusproto.RawRecordWithId) (list.AclList, *consensusproto.RawRecordWithId) {
	t.Helper()
	if records == nil {
		metadataKey, _, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		created, err := spacepayloads.StoragePayloadForSpaceCreateV1(spacepayloads.SpaceCreatePayload{
			SigningKey:  keys.SignKey,
			MasterKey:   keys.SignKey,
			ReadKey:     nil,
			MetadataKey: metadataKey,
			SpaceType:   "test",
			Metadata:    []byte("owner"),
		})
		require.NoError(t, err)
		records = []*consensusproto.RawRecordWithId{created.AclWithId}
	}
	storage, err := list.NewInMemoryStorage(records[0].Id, records)
	require.NoError(t, err)
	aclList, err := list.BuildAclListWithIdentity(keys, storage, recordverifier.NewValidateFull())
	require.NoError(t, err)
	return aclList, records[0]
}

func newPlaintextSide(
	t *testing.T, name string, keys *accountdata.AccountKeys,
	aclRecords []*consensusproto.RawRecordWithId, root *treechangeproto.RawTreeChangeWithId,
) *plaintextSide {
	t.Helper()
	ctx := context.Background()
	aclList, aclRoot := newKeylessAcl(t, keys, aclRecords)
	if root == nil {
		var err error
		root, err = objecttree.CreateObjectTreeRoot(objecttree.ObjectTreeCreatePayload{
			PrivKey:    keys.SignKey,
			ChangeType: "test",
			SpaceId:    "spaceId",
		}, aclList)
		require.NoError(t, err)
	}
	db := newPlaintextStore(t, name)
	changesColl, err := db.Collection(ctx, objecttree.CollName)
	require.NoError(t, err)
	require.NoError(t, changesColl.EnsureIndex(ctx, anystore.IndexInfo{
		Fields: []string{objecttree.TreeKey, objecttree.OrderKey},
		Unique: true,
	}))
	heads, err := headstorage.New(ctx, db)
	require.NoError(t, err)
	st, err := objecttree.CreateStorage(ctx, root, heads, db)
	require.NoError(t, err)
	// 与 exporter 一致：给存储装上 AddSeq 计数器，否则 AddContent 落盘时空指针。
	st.(interface{ SetAddSeq(seq *atomic.Uint64) }).SetAddSeq(&atomic.Uint64{})
	tree, err := objecttree.BuildObjectTree(st, aclList)
	require.NoError(t, err)
	return &plaintextSide{aclList: aclList, tree: tree, aclRoot: aclRoot}
}

func TestPlaintextSyncBetweenSameAccountNodes(t *testing.T) {
	ctx := context.Background()
	// 同账号：A、B 持同一份账户密钥（对应“两个同账号测试节点”）。
	keys, err := accountdata.NewRandom()
	require.NoError(t, err)

	// A 建无密钥空间，写一段纯文字（ShouldBeEncrypted=false 即明文变更）。
	a := newPlaintextSide(t, "a.db", keys, nil, nil)
	a.tree.Lock()
	added, err := a.tree.AddContent(ctx, objecttree.SignableChangeContent{
		Data:              []byte(plaintextTestText),
		Key:               keys.SignKey,
		ShouldBeEncrypted: false,
		DataType:          "test",
	})
	a.tree.Unlock()
	require.NoError(t, err)
	require.Len(t, added.Added, 1)

	// 抓同步存储中的新变更：载荷为明文、ReadKeyId 为空（验签打开）。
	raw := added.Added[0].RawTreeChangeWithId()
	builder := objecttree.NewChangeBuilder(crypto.NewKeyStorage(), a.tree.Header())
	stored, err := builder.Unmarshall(raw, true)
	require.NoError(t, err)
	require.Equal(t, "", stored.ReadKeyId)
	require.Equal(t, plaintextTestText, string(stored.Data))

	// B 从 ACL 记录 + 同步变更重建（模拟对 heads 补缺），读出同样明文。
	aclRecords, err := a.aclList.RecordsAfter(ctx, a.aclList.Head().Id)
	require.NoError(t, err)
	aclRecords = append([]*consensusproto.RawRecordWithId{a.aclRoot}, aclRecords...)
	b := newPlaintextSide(t, "b.db", keys, aclRecords, a.tree.Header())
	b.tree.Lock()
	syncRes, err := b.tree.AddRawChanges(ctx, objecttree.RawChangesPayload{
		NewHeads:   added.Heads,
		RawChanges: []*treechangeproto.RawTreeChangeWithId{raw},
	})
	require.NoError(t, err)
	require.Equal(t, added.Heads, syncRes.Heads)
	require.Equal(t, added.Heads, b.tree.Heads())
	var got []string
	iterErr := b.tree.IterateRoot(
		func(change *objecttree.Change, decrypted []byte) (any, error) {
			got = append(got, string(decrypted))
			return nil, nil
		},
		func(change *objecttree.Change) bool {
			// 根变更无数据，跳过收集（convert 侧已处理）。
			return true
		})
	b.tree.Unlock()
	require.NoError(t, iterErr)
	require.Equal(t, []string{plaintextTestText}, got)
}
