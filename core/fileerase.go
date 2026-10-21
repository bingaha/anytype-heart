package core

import (
	"context"
	"fmt"

	"github.com/anyproto/anytype-heart/core/files/fileobject"
	"github.com/anyproto/anytype-heart/pb"
)

// fileerase.go 是 08 号票的新增文件（加法）：设置页/文件菜单的"立即擦除"
// 按钮经 FileEraseData RPC 落到这里，直调 05 号票的
// fileobject.EraseFileData（跳过删除保留期，立刻清本地与备份节点字节）。

func (mw *Middleware) FileEraseData(cctx context.Context, req *pb.RpcFileEraseDataRequest) *pb.RpcFileEraseDataResponse {
	response := func(code pb.RpcFileEraseDataResponseErrorCode, err error) *pb.RpcFileEraseDataResponse {
		m := &pb.RpcFileEraseDataResponse{Error: &pb.RpcFileEraseDataResponseError{Code: code}}
		if err != nil {
			m.Error.Description = getErrorDescription(err)
		}
		return m
	}
	if req.GetSpaceId() == "" || req.GetObjectId() == "" {
		return response(pb.RpcFileEraseDataResponseError_BAD_INPUT, fmt.Errorf("spaceId and objectId are required"))
	}
	if mw.applicationService.GetApp() == nil {
		return response(pb.RpcFileEraseDataResponseError_UNKNOWN_ERROR, ErrNotLoggedIn)
	}
	if err := mustService[fileobject.Service](mw).EraseFileData(req.GetSpaceId(), req.GetObjectId()); err != nil {
		return response(pb.RpcFileEraseDataResponseError_UNKNOWN_ERROR, err)
	}
	return response(pb.RpcFileEraseDataResponseError_NULL, nil)
}
