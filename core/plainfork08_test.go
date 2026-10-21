package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/anytype-heart/pb"
	"github.com/anyproto/anytype-heart/space/spacecore"
)

// 08 号票收尾单测：三入口 RPC 的参数验证层（零值 Middleware 即可跑，
// 不依赖 app fixture；重逻辑已在 05/07 号票的 service 层单测覆盖）。
// 链路对应：擦除按钮→FileEraseData、口令→NetworkPassphraseSet、
// 备份目录→BackupHealthExportNow。

func TestFileEraseData_BadInput(t *testing.T) {
	mw := &Middleware{}
	ctx := context.Background()

	for _, req := range []*pb.RpcFileEraseDataRequest{
		{},
		{SpaceId: "space1"},
		{ObjectId: "obj1"},
	} {
		resp := mw.FileEraseData(ctx, req)
		assert.Equal(t, pb.RpcFileEraseDataResponseError_BAD_INPUT, resp.Error.Code)
	}
}

func TestNetworkPassphraseSet_BadInputAndRoundTrip(t *testing.T) {
	mw := &Middleware{}
	ctx := context.Background()

	resp := mw.NetworkPassphraseSet(ctx, &pb.RpcNetworkPassphraseSetRequest{Passphrase: ""})
	assert.Equal(t, pb.RpcNetworkPassphraseSetResponseError_BAD_INPUT, resp.Error.Code)

	defer spacecore.ClearWholeNetworkPassphrase()
	resp = mw.NetworkPassphraseSet(ctx, &pb.RpcNetworkPassphraseSetRequest{Passphrase: "test-pp-08"})
	assert.Equal(t, pb.RpcNetworkPassphraseSetResponseError_NULL, resp.Error.Code)
	assert.True(t, resp.Configured)
	assert.NotEmpty(t, resp.DiscoveryServiceName)

	st := mw.NetworkPassphraseStatus(ctx, &pb.RpcNetworkPassphraseStatusRequest{})
	assert.Equal(t, pb.RpcNetworkPassphraseStatusResponseError_NULL, st.Error.Code)
	assert.True(t, st.Configured)
	assert.Equal(t, resp.DiscoveryServiceName, st.DiscoveryServiceName)
}

func TestBackupHealthExportNow_BadInput(t *testing.T) {
	mw := &Middleware{}

	resp := mw.BackupHealthExportNow(context.Background(), &pb.RpcBackupHealthExportNowRequest{DestDir: ""})
	assert.Equal(t, pb.RpcBackupHealthExportNowResponseError_BAD_INPUT, resp.Error.Code)
}
