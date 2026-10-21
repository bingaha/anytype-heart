package core

import (
	"context"
	"errors"

	"github.com/anyproto/anytype-heart/pb"
	"github.com/anyproto/anytype-heart/space/spacecore"
)

// networkpassphrase.go 是 08 号票的新增文件（加法）：设置页"整网口令"
// 入口的 RPC 面。Set 把前端经 keytar 读出的口令送入后端，一处设置同时
// 喂给 04 号票握手加盐与 01/08 号票发现后缀（见
// spacecore.SetWholeNetworkPassphrase）；Status 只回显有无与发现名，
// 永不回显口令本身。

var errBackupDirRequired = errors.New("destDir is required")

// NetworkPassphraseSet 安装整网口令。空口令拒绝（BAD_INPUT）：清口令即
// 重组网，用新口令覆盖即可，无需"清空"语义。
func (mw *Middleware) NetworkPassphraseSet(cctx context.Context, req *pb.RpcNetworkPassphraseSetRequest) *pb.RpcNetworkPassphraseSetResponse {
	response := func(code pb.RpcNetworkPassphraseSetResponseErrorCode, err error) *pb.RpcNetworkPassphraseSetResponse {
		m := &pb.RpcNetworkPassphraseSetResponse{
			Error:               &pb.RpcNetworkPassphraseSetResponseError{Code: code},
			Configured:          spacecore.WholeNetworkPassphraseConfigured(),
			DiscoveryServiceName: spacecore.DiscoveryServiceName(),
		}
		if err != nil {
			m.Error.Description = getErrorDescription(err)
		}
		return m
	}
	if req.GetPassphrase() == "" {
		return response(pb.RpcNetworkPassphraseSetResponseError_BAD_INPUT, errors.New("passphrase is required"))
	}
	// 口令只存内存（持久化在系统钥匙串，前端负责），后端不落盘。
	spacecore.SetWholeNetworkPassphrase(req.GetPassphrase())
	return response(pb.RpcNetworkPassphraseSetResponseError_NULL, nil)
}

// NetworkPassphraseStatus 回显口令配置状态与当前发现名（供设置页展示与
// "重启后发现名带后缀"验收；不回显口令）。
func (mw *Middleware) NetworkPassphraseStatus(cctx context.Context, req *pb.RpcNetworkPassphraseStatusRequest) *pb.RpcNetworkPassphraseStatusResponse {
	return &pb.RpcNetworkPassphraseStatusResponse{
		Error:               &pb.RpcNetworkPassphraseStatusResponseError{Code: pb.RpcNetworkPassphraseStatusResponseError_NULL},
		Configured:          spacecore.WholeNetworkPassphraseConfigured(),
		DiscoveryServiceName: spacecore.DiscoveryServiceName(),
	}
}
