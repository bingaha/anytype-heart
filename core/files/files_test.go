package files

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonfile/fileservice"
	"github.com/globalsign/mgo/bson"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/anytype-heart/core/block/editor/state"
	"github.com/anyproto/anytype-heart/core/domain"
	"github.com/anyproto/anytype-heart/core/event/mock_event"
	"github.com/anyproto/anytype-heart/core/files/fileobject/filemodels"
	"github.com/anyproto/anytype-heart/core/files/filestorage"
	"github.com/anyproto/anytype-heart/core/files/filesync"
	"github.com/anyproto/anytype-heart/core/files/filesync/mock_filesync"
	"github.com/anyproto/anytype-heart/pkg/lib/bundle"
	"github.com/anyproto/anytype-heart/pkg/lib/localstore/objectstore"
	"github.com/anyproto/anytype-heart/pkg/lib/mill/schema"
	"github.com/anyproto/anytype-heart/pkg/lib/pb/storage"
	"github.com/anyproto/anytype-heart/tests/testutil"
)

type fixture struct {
	Service

	eventSender       *mock_event.MockSender
	commonFileService fileservice.FileService
	fileSyncService   filesync.FileSync
	objectStore       objectstore.ObjectStore
}

const (
	spaceId         = "space1"
	testFileName    = "myFile"
	testFileContent = "it's my favorite file"
)

func newFixture(t *testing.T) *fixture {
	blockStorage := filestorage.NewInMemory()

	commonFileService := fileservice.New()
	fileSyncService := mock_filesync.NewMockFileSync(t)
	fileSyncService.EXPECT().AddFile(mock.Anything).Return(nil).Maybe()

	objectStore := objectstore.NewStoreFixture(t)

	ctx := context.Background()
	a := new(app.App)
	a.Register(commonFileService)
	a.Register(testutil.PrepareMock(ctx, a, fileSyncService))
	a.Register(blockStorage)
	a.Register(objectStore)

	err := commonFileService.Init(a)
	require.NoError(t, err)

	s := New()
	err = s.Init(a)
	require.NoError(t, err)

	return &fixture{
		Service:           s,
		commonFileService: commonFileService,
		fileSyncService:   fileSyncService,
		objectStore:       objectStore,
	}
}

func TestFileAdd(t *testing.T) {
	fx, got := getFixtureAndFileInfo(t)
	ctx := context.Background()

	require.Len(t, got.Variants, 1)

	var variantCid string

	t.Run("expect decrypting content", func(t *testing.T) {
		assertFileMeta(t, got, got.Variants)

		variant := got.Variants[0]

		variantCid = variant.Hash
		reader, err := fx.GetContentReader(ctx, spaceId, variantCid, got.EncryptionKeys.EncryptionKeys[variant.Path])
		require.NoError(t, err)

		gotContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, testFileContent, string(gotContent))

	})

	t.Run("expect that plaintext content stored in DAG", func(t *testing.T) {
		contentCid := cid.MustParse(variantCid)
		// 整文件一块：内容块为 raw codec 单块
		require.Equal(t, uint64(cid.Raw), contentCid.Prefix().Codec)
		plainContent, err := fx.commonFileService.GetFile(ctx, contentCid)
		require.NoError(t, err)
		gotPlainContent, err := io.ReadAll(plainContent)
		require.NoError(t, err)
		assert.Equal(t, testFileContent, string(gotPlainContent))
	})
}

func getFixtureAndFileInfo(t *testing.T) (*fixture, *AddResult) {
	fx := newFixture(t)
	ctx := context.Background()

	lastModifiedDate := time.Now()
	buf := strings.NewReader(testFileContent)

	opts := []AddOption{
		WithName(testFileName),
		WithLastModifiedDate(lastModifiedDate.Unix()),
		WithReader(buf),
	}
	got, err := fx.FileAdd(ctx, spaceId, opts...)
	require.NoError(t, err)
	assert.NotEmpty(t, got.FileId)
	got.Commit()
	return fx, got
}

func TestIndexFile(t *testing.T) {
	t.Run("with encryption keys available", func(t *testing.T) {
		fx := newFixture(t)

		fileResult := testAddFile(t, fx)

		// Index
		file, err := fx.GetFileVariants(context.Background(), domain.FullFileId{FileId: fileResult.FileId, SpaceId: spaceId}, fileResult.EncryptionKeys.EncryptionKeys)
		require.NoError(t, err)

		assertFileMeta(t, fileResult, file)
	})

	t.Run("without encryption keys", func(t *testing.T) {
		fx := newFixture(t)

		fileResult := testAddFile(t, fx)

		// 明文存放：无密钥也能读出元数据
		variants, err := fx.GetFileVariants(context.Background(), domain.FullFileId{FileId: fileResult.FileId, SpaceId: spaceId}, nil)
		require.NoError(t, err)
		assertFileMeta(t, fileResult, variants)
	})
}

func assertFileMeta(t *testing.T, fileResult *AddResult, variants []*storage.FileInfo) {
	for _, v := range variants {
		assert.Equal(t, fileResult.MIME, v.Media)
		assert.Equal(t, testFileName, v.Name)
		assert.Equal(t, int64(len(testFileContent)), v.Size_)
		now := time.Now()
		if !assert.True(t, now.Sub(time.Unix(v.LastModifiedDate, 0)) < time.Second) {
			fmt.Println(now)
			fmt.Println(time.Unix(v.LastModifiedDate, 0))
		}
		assert.True(t, now.Sub(time.Unix(v.Added, 0)) < time.Second)
	}
}

func TestFileAddWithCustomKeys(t *testing.T) {
	// 明文全量同步：传入的自定义加密密钥一律忽略，内容始终明文存放，Key 留空。
	t.Run("custom keys are ignored, content stays plaintext", func(t *testing.T) {
		for i, customKeys := range []map[string]string{
			{encryptionKeyPath(schema.LinkFile): "bweokjjonr756czpdoymdfwzromqtqb27z44tmcb2vv322y2v62ja"},
			nil,
			{"invalid": "key"},
			{encryptionKeyPath(schema.LinkFile): "not-an-aes-key"},
		} {
			t.Run(fmt.Sprintf("case %d", i+1), func(t *testing.T) {
				fx := newFixture(t)
				ctx := context.Background()

				lastModifiedDate := time.Now()
				buf := strings.NewReader(testFileContent)

				opts := []AddOption{
					WithName(testFileName),
					WithLastModifiedDate(lastModifiedDate.Unix()),
					WithReader(buf),
					WithCustomEncryptionKeys(customKeys),
				}
				got, err := fx.FileAdd(ctx, spaceId, opts...)
				require.NoError(t, err)
				assert.NotEmpty(t, got.FileId)
				got.Commit()

				assertCustomEncryptionKeys(t, fx, got, customKeys)

				// 无需任何密钥即可读出原文
				reader, err := fx.GetContentReader(ctx, spaceId, got.Variants[0].Hash, "")
				require.NoError(t, err)
				content, err := io.ReadAll(reader)
				require.NoError(t, err)
				assert.Equal(t, testFileContent, string(content))
			})
		}
	})
}

func TestAddFilesConcurrently(t *testing.T) {
	testAddConcurrently(t, func(t *testing.T, fx *fixture) *AddResult {
		return testAddFile(t, fx)
	})
}

func testAddConcurrently(t *testing.T, addFunc func(t *testing.T, fx *fixture) *AddResult) {
	fx := newFixture(t)

	const numTimes = 5
	gotCh := make(chan *AddResult, numTimes)

	for i := 0; i < numTimes; i++ {
		go func() {
			got := addFunc(t, fx)
			gotCh <- got
		}()
	}

	// The per-checksum add lock is released at the end of FileAdd/ImageAdd (it is no
	// longer held until Commit) to avoid a deadlock between a preloaded upload and a
	// subsequent upload of the same checksum without a preloadId. As a consequence,
	// concurrent adds of the same content may each create a separate file object, so we
	// no longer assert strict deduplication here - only that every concurrent add
	// succeeds without deadlocking and reports consistent content (MIME and size).
	var prev *AddResult
	for i := 0; i < numTimes; i++ {
		got := <-gotCh
		require.NotNil(t, got)
		assert.NotEmpty(t, got.FileId)

		if prev == nil {
			prev = got
		} else {
			assert.Equal(t, prev.MIME, got.MIME)
			assert.Equal(t, prev.Size, got.Size)
		}
	}
}

func testAddFile(t *testing.T, fx *fixture) *AddResult {
	lastModifiedDate := time.Now()
	buf := strings.NewReader(testFileContent)
	opts := []AddOption{
		WithName(testFileName),
		WithLastModifiedDate(lastModifiedDate.Unix()),
		WithReader(buf),
	}
	got, err := fx.FileAdd(context.Background(), spaceId, opts...)
	require.NoError(t, err)

	fx.addFileObjectToStore(t, got)

	got.Commit()

	return got
}

func (fx *fixture) addFileObjectToStore(t *testing.T, got *AddResult) {
	fullFileId := domain.FullFileId{
		SpaceId: spaceId,
		FileId:  got.FileId,
	}

	file, err := NewFile(fx.Service, fullFileId, got.Variants)
	require.NoError(t, err)

	objectId := bson.NewObjectId().Hex()
	st := state.NewDoc(objectId, nil).(*state.State)
	st.SetFileInfo(state.FileInfo{
		FileId:         got.FileId,
		EncryptionKeys: got.EncryptionKeys.EncryptionKeys,
	})
	details, _, err := file.Details(context.Background())
	require.NoError(t, err)

	st.SetDetails(details)
	st.SetDetailAndBundledRelation(bundle.RelationKeyFileId, domain.String(got.FileId))
	err = filemodels.InjectVariantsToDetails(got.Variants, st)
	require.NoError(t, err)

	err = fx.objectStore.SpaceIndex(spaceId).UpdateObjectDetails(context.Background(), objectId, st.CombinedDetails())
	require.NoError(t, err)
}
