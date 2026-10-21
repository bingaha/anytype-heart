package filedownloader

// 明文全量同步（06）：清单分片下载的断点续传断言。
// 用可注错的 DAG 双件模拟远端 + 代理缓存语义：中途掐断一次，恢复后只需补
// 缺失分片，最终字节与源一致；小文件仍走一次到位原路径。

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync/app"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"

	"github.com/anyproto/anytype-heart/core/domain"
	"github.com/anyproto/anytype-heart/core/files/chunkspec"
)

// flakyDAG simulates the proxy blockstore semantics over separate local and
// remote maps: a remote hit is cached locally and counted; programmed
// failures emulate a network cut.
type flakyDAG struct {
	mu            sync.Mutex
	local         map[string]ipld.Node
	remote        map[string]ipld.Node
	failRemaining map[string]int
	remoteFetches map[string]int
	// remoteAttempts counts every remote miss (including failed attempts).
	remoteAttempts map[string]int
}

func newFlakyDAG() *flakyDAG {
	return &flakyDAG{
		local:          map[string]ipld.Node{},
		remote:         map[string]ipld.Node{},
		failRemaining:  map[string]int{},
		remoteFetches:  map[string]int{},
		remoteAttempts: map[string]int{},
	}
}

func (f *flakyDAG) putRemote(n ipld.Node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.remote[n.Cid().KeyString()] = n
}

func (f *flakyDAG) failNext(key string, times int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failRemaining[key] = times
}

func (f *flakyDAG) remoteFetchCount(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.remoteFetches[key]
}

func (f *flakyDAG) remoteAttemptCount(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.remoteAttempts[key]
}

func (f *flakyDAG) localHas(c cid.Cid) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.local[c.KeyString()]
	return ok
}

func (f *flakyDAG) Get(_ context.Context, c cid.Cid) (ipld.Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := c.KeyString()
	if n, ok := f.local[key]; ok {
		return n, nil
	}
	f.remoteAttempts[key]++
	if f.failRemaining[key] > 0 {
		f.failRemaining[key]--
		return nil, fmt.Errorf("simulated network cut for %s", c)
	}
	n, ok := f.remote[key]
	if !ok {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	f.local[key] = n
	f.remoteFetches[key]++
	return n, nil
}

func (f *flakyDAG) GetMany(ctx context.Context, ks []cid.Cid) <-chan *ipld.NodeOption {
	out := make(chan *ipld.NodeOption, len(ks))
	go func() {
		defer close(out)
		for _, k := range ks {
			select {
			case <-ctx.Done():
				return
			default:
			}
			n, err := f.Get(ctx, k)
			if err != nil {
				continue
			}
			out <- &ipld.NodeOption{Node: n}
		}
	}()
	return out
}

func (f *flakyDAG) Add(_ context.Context, n ipld.Node) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.local[n.Cid().KeyString()] = n
	return nil
}

func (f *flakyDAG) AddMany(ctx context.Context, ns []ipld.Node) error {
	for _, n := range ns {
		if err := f.Add(ctx, n); err != nil {
			return err
		}
	}
	return nil
}

func (f *flakyDAG) Remove(_ context.Context, _ cid.Cid) error { return nil }

func (f *flakyDAG) RemoveMany(_ context.Context, _ []cid.Cid) error { return nil }

// stubFileService exposes the flaky DAG through the fileservice interface
// the downloader expects.
type stubFileService struct {
	dag *flakyDAG
}

func (s *stubFileService) Name() string                   { return "stub" }
func (s *stubFileService) Init(_ *app.App) error          { return nil }
func (s *stubFileService) Run(_ context.Context) error    { return nil }
func (s *stubFileService) Close(_ context.Context) error  { return nil }
func (s *stubFileService) DAGService() ipld.DAGService    { return s.dag }
func (s *stubFileService) AddFile(_ context.Context, _ io.Reader) (ipld.Node, error) {
	return nil, errors.New("unused")
}
func (s *stubFileService) GetFile(_ context.Context, _ cid.Cid) (ufsio.ReadSeekCloser, error) {
	return nil, errors.New("unused")
}
func (s *stubFileService) HasCid(_ context.Context, c cid.Cid) (bool, error) {
	return s.dag.localHas(c), nil
}

func newTransferTestService(dag *flakyDAG) *service {
	s := New().(*service)
	stub := &stubFileService{dag: dag}
	s.commonFile = stub
	s.dagService = stub.DAGService()
	return s
}

// buildChunkedPair puts a heart-style pair DAG (meta + whole + manifest) into
// the remote side only, returning source bytes and all CIDs.
func buildChunkedPair(t *testing.T, dag *flakyDAG, size int) (content []byte, pairCid, contentCid, metaCid cid.Cid, chunkCids []cid.Cid) {
	t.Helper()
	content = make([]byte, size)
	_, err := rand.Read(content)
	require.NoError(t, err)

	whole := merkledag.NewRawNode(content)
	dag.putRemote(whole)
	contentCid = whole.Cid()

	meta := merkledag.NewRawNode([]byte(`{"name":"big.bin"}`))
	dag.putRemote(meta)
	metaCid = meta.Cid()

	cs := chunkspec.ChunkSizeBytes()
	var chunkNodes []ipld.Node
	for off := int64(0); off < int64(len(content)); off += cs {
		end := min(off+cs, int64(len(content)))
		node := merkledag.NewRawNode(content[off:end])
		dag.putRemote(node)
		chunkCids = append(chunkCids, node.Cid())
		chunkNodes = append(chunkNodes, node)
	}
	require.Greater(t, len(chunkCids), 1)

	pair := new(merkledag.ProtoNode)
	require.NoError(t, pair.AddNodeLink(chunkspec.MetaLinkName, meta))
	require.NoError(t, pair.AddNodeLink(chunkspec.ContentLinkName, whole))
	for i, node := range chunkNodes {
		// Re-link by CID so the manifest matches what the writer stores.
		require.NoError(t, pair.AddRawLink(chunkspec.ChunkLinkName(i), &ipld.Link{
			Name: chunkspec.ChunkLinkName(i),
			Cid:  node.Cid(),
			Size: uint64(len(node.RawData())),
		}))
	}
	// Real file DAGs nest the pair under an outer directory.
	outer := new(merkledag.ProtoNode)
	require.NoError(t, outer.AddNodeLink("0", pair))
	dag.putRemote(pair)
	dag.putRemote(outer)
	return content, outer.Cid(), contentCid, metaCid, chunkCids
}

func TestChunkedDownloadResumesAfterCut(t *testing.T) {
	for _, size := range []int{2 << 20, 70 << 20} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			dag := newFlakyDAG()
			content, pairCid, contentCid, _, chunkCids := buildChunkedPair(t, dag, size)
			svc := newTransferTestService(dag)

			// 中途掐断一次：第三个分片首次拉取失败
			dag.failNext(chunkCids[2].KeyString(), 1)

			task := downloadTask{
				objectId: "obj1",
				spaceId:  "space1",
				fileId:   domain.FileId(pairCid.String()),
			}
			completed, err := svc.downloadFileResumable(context.Background(), task)
			require.NoError(t, err, "resume must complete after one cut")
			assert.Equal(t, int64(len(content)), completed)

			// 落盘仍是一整块：content 整块存在且字节与源一致
			require.True(t, dag.localHas(contentCid), "assembled whole block must be stored")
			whole, err := dag.Get(context.Background(), contentCid)
			require.NoError(t, err)
			assert.Equal(t, content, whole.RawData())

			// 续传语义：被掐断的分片补拉一次，其余分片只拉一次（无整文件重传）
			assert.Equal(t, 2, dag.remoteAttemptCount(chunkCids[2].KeyString()))
			assert.Equal(t, 1, dag.remoteFetchCount(chunkCids[2].KeyString()))
			for i, c := range chunkCids {
				if i == 2 {
					continue
				}
				assert.Equal(t, 1, dag.remoteFetchCount(c.KeyString()), "chunk %d must be fetched exactly once", i)
			}
		})
	}
}

func TestSmallFileOneShot(t *testing.T) {
	dag := newFlakyDAG()
	content := []byte("small file, one shot")

	whole := merkledag.NewRawNode(content)
	dag.putRemote(whole)
	meta := merkledag.NewRawNode([]byte(`{}`))
	dag.putRemote(meta)
	pair := new(merkledag.ProtoNode)
	require.NoError(t, pair.AddNodeLink(chunkspec.MetaLinkName, meta))
	require.NoError(t, pair.AddNodeLink(chunkspec.ContentLinkName, whole))
	dag.putRemote(pair)

	svc := newTransferTestService(dag)
	completed, err := svc.downloadFileResumable(context.Background(), downloadTask{
		objectId: "obj-small",
		spaceId:  "space1",
		fileId:   domain.FileId(pair.Cid().String()),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), completed, "legacy path reports no chunked progress")

	// 一次到位：每块恰好拉取一次
	assert.Equal(t, 1, dag.remoteFetchCount(pair.Cid().KeyString()))
	assert.Equal(t, 1, dag.remoteFetchCount(meta.Cid().KeyString()))
	assert.Equal(t, 1, dag.remoteFetchCount(whole.Cid().KeyString()))
	got, err := dag.Get(context.Background(), whole.Cid())
	require.NoError(t, err)
	assert.Equal(t, content, got.RawData())
}

func TestWorkerRetriesThenDrops(t *testing.T) {
	oldDelay := downloadRetryDelay
	downloadRetryDelay = func(int) time.Duration { return time.Millisecond }
	defer func() { downloadRetryDelay = oldDelay }()

	svc := New().(*service)
	d := svc.newDownloader(0)
	var calls atomic.Int64
	d.handleTask = func(context.Context, downloadTask) (int64, error) {
		calls.Add(1)
		return 1234, errors.New("persistent failure")
	}

	go d.runManager()
	defer d.stop()
	go d.runDownloadWorker()

	d.addTaskCh <- downloadTask{objectId: "poison", spaceId: "s", fileId: "f"}

	require.Eventually(t, func() bool {
		return calls.Load() == int64(maxDownloadAttempts+1)
	}, 10*time.Second, 5*time.Millisecond, "task must be retried then dropped")

	// 耗尽后不再调度
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int64(maxDownloadAttempts+1), calls.Load())
}
