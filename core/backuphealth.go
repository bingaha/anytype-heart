package core

import (
	"context"

	"github.com/anyproto/anytype-heart/core/backuphealth"
	"github.com/anyproto/anytype-heart/pb"
)

// backuphealth.go 是 08 号票的新增文件（加法）：设置页"全量状态"入口与
// 自检失败弹窗的数据面。四个 RPC 全部透传 07 号票 backuphealth.Service
// 的现成方法（LastQuickReport/LastDeepReport/LastExport、QuickCheckNow、
// DeepVerify、ExportAllSpacesNow），本层只做 nil 防护与 proto 映射。

func (mw *Middleware) backupHealthService() (backuphealth.Service, error) {
	return getService[backuphealth.Service](mw)
}

func backupHealthQuickReportToProto(rep *backuphealth.QuickReport) *pb.RpcBackupHealthQuickReport {
	if rep == nil {
		return nil
	}
	out := &pb.RpcBackupHealthQuickReport{
		AtUnixMilli: rep.At.UnixMilli(),
		DurationMs:  rep.DurationMs,
		Ok:          rep.OK,
		Failures:    rep.Failures,
		Files: &pb.RpcBackupHealthFileCheck{
			Dir:         rep.Files.Dir,
			Skipped:     rep.Files.Skipped,
			FileCount:   int32(rep.Files.FileCount),
			TotalBytes:  rep.Files.TotalBytes,
			NewestFile:  rep.Files.NewestFile,
			NewestSize:  rep.Files.NewestSize,
			NewestMtime: rep.Files.NewestMtime,
		},
	}
	for _, d := range rep.Dbs {
		out.Dbs = append(out.Dbs, &pb.RpcBackupHealthDbCheck{
			SpaceId:    d.SpaceId,
			Kind:       d.Kind,
			Path:       d.Path,
			Size_:      d.Size,
			Tables:     d.Tables,
			Mismatches: d.Mismatches,
			Opened:     d.Opened,
			QuickCheck: d.QuickCheck,
		})
	}
	return out
}

func backupHealthDeepReportToProto(rep *backuphealth.DeepReport) *pb.RpcBackupHealthDeepReport {
	if rep == nil {
		return nil
	}
	out := &pb.RpcBackupHealthDeepReport{
		AtUnixMilli: rep.At.UnixMilli(),
		DurationMs:  rep.DurationMs,
		Ok:          rep.OK,
		Failures:    rep.Failures,
		Files: &pb.RpcBackupHealthDeepFileReport{
			Dir:         rep.Files.Dir,
			Skipped:     rep.Files.Skipped,
			FileCount:   int32(rep.Files.FileCount),
			TotalBytes:  rep.Files.TotalBytes,
			RootHex:     rep.Files.RootHex,
			FileHashes:  rep.Files.FileHashes,
		},
	}
	for _, d := range rep.Dbs {
		out.Dbs = append(out.Dbs, &pb.RpcBackupHealthDeepDbReport{
			SpaceId:        d.SpaceId,
			Kind:           d.Kind,
			Path:           d.Path,
			Size_:          d.Size,
			IntegrityCheck: d.IntegrityCheck,
			TableHashes:    d.TableHashes,
		})
	}
	return out
}

func backupHealthExportSummaryToProto(sum *backuphealth.ExportSummary) *pb.RpcBackupHealthExportSummary {
	if sum == nil {
		return nil
	}
	out := &pb.RpcBackupHealthExportSummary{
		AtUnixMilli: sum.At.UnixMilli(),
		Dest:        sum.Dest,
		DurationMs:  sum.DurationMs,
	}
	for _, s := range sum.Spaces {
		out.Spaces = append(out.Spaces, &pb.RpcBackupHealthSpaceExport{
			SpaceId:         s.SpaceId,
			SpaceName:       s.SpaceName,
			Dir:             s.Dir,
			MdCount:         int32(s.MdCount),
			AttachmentCount: int32(s.AttachmentCount),
			Bytes:           s.Bytes,
			Skipped:         s.Skipped,
			Error:           s.Error,
		})
	}
	return out
}

// BackupHealthLastReport 查询最近一次自检/深校验/导出报告（均可能为 nil，
// 表示尚未跑过）。退出前快速自检失败时由 07 号票 StateChange 落盘并记
// ERROR 日志，前端据此弹窗——数据源就是这里。
func (mw *Middleware) BackupHealthLastReport(cctx context.Context, req *pb.RpcBackupHealthLastReportRequest) *pb.RpcBackupHealthLastReportResponse {
	response := func(code pb.RpcBackupHealthLastReportResponseErrorCode, err error) *pb.RpcBackupHealthLastReportResponse {
		m := &pb.RpcBackupHealthLastReportResponse{Error: &pb.RpcBackupHealthLastReportResponseError{Code: code}}
		if err != nil {
			m.Error.Description = getErrorDescription(err)
		}
		return m
	}
	svc, err := mw.backupHealthService()
	if err != nil {
		return response(pb.RpcBackupHealthLastReportResponseError_UNKNOWN_ERROR, err)
	}
	m := response(pb.RpcBackupHealthLastReportResponseError_NULL, nil)
	m.Quick = backupHealthQuickReportToProto(svc.LastQuickReport())
	m.Deep = backupHealthDeepReportToProto(svc.LastDeepReport())
	m.Export = backupHealthExportSummaryToProto(svc.LastExport())
	return m
}

// BackupHealthQuickCheck 手动触发一次快速自检（毫秒级阻塞，设置页"检查备份"按钮用）。
func (mw *Middleware) BackupHealthQuickCheck(cctx context.Context, req *pb.RpcBackupHealthQuickCheckRequest) *pb.RpcBackupHealthQuickCheckResponse {
	response := func(code pb.RpcBackupHealthQuickCheckResponseErrorCode, err error) *pb.RpcBackupHealthQuickCheckResponse {
		m := &pb.RpcBackupHealthQuickCheckResponse{Error: &pb.RpcBackupHealthQuickCheckResponseError{Code: code}}
		if err != nil {
			m.Error.Description = getErrorDescription(err)
		}
		return m
	}
	svc, err := mw.backupHealthService()
	if err != nil {
		return response(pb.RpcBackupHealthQuickCheckResponseError_UNKNOWN_ERROR, err)
	}
	m := response(pb.RpcBackupHealthQuickCheckResponseError_NULL, nil)
	m.Report = backupHealthQuickReportToProto(svc.QuickCheckNow(cctx))
	return m
}

// BackupHealthDeepVerify 手动触发一次全量深校验（空闲或设置页手动触发）。
func (mw *Middleware) BackupHealthDeepVerify(cctx context.Context, req *pb.RpcBackupHealthDeepVerifyRequest) *pb.RpcBackupHealthDeepVerifyResponse {
	response := func(code pb.RpcBackupHealthDeepVerifyResponseErrorCode, err error) *pb.RpcBackupHealthDeepVerifyResponse {
		m := &pb.RpcBackupHealthDeepVerifyResponse{Error: &pb.RpcBackupHealthDeepVerifyResponseError{Code: code}}
		if err != nil {
			m.Error.Description = getErrorDescription(err)
		}
		return m
	}
	svc, err := mw.backupHealthService()
	if err != nil {
		return response(pb.RpcBackupHealthDeepVerifyResponseError_UNKNOWN_ERROR, err)
	}
	m := response(pb.RpcBackupHealthDeepVerifyResponseError_NULL, nil)
	m.Report = backupHealthDeepReportToProto(svc.DeepVerify(cctx))
	return m
}

// BackupHealthExportNow 手动触发一次全量明文导出（设置页"立即导出"按钮用，调 07 号票接口）。
func (mw *Middleware) BackupHealthExportNow(cctx context.Context, req *pb.RpcBackupHealthExportNowRequest) *pb.RpcBackupHealthExportNowResponse {
	response := func(code pb.RpcBackupHealthExportNowResponseErrorCode, err error) *pb.RpcBackupHealthExportNowResponse {
		m := &pb.RpcBackupHealthExportNowResponse{Error: &pb.RpcBackupHealthExportNowResponseError{Code: code}}
		if err != nil {
			m.Error.Description = getErrorDescription(err)
		}
		return m
	}
	if req.GetDestDir() == "" {
		return response(pb.RpcBackupHealthExportNowResponseError_BAD_INPUT, errBackupDirRequired)
	}
	svc, err := mw.backupHealthService()
	if err != nil {
		if err == ErrNotLoggedIn {
			return response(pb.RpcBackupHealthExportNowResponseError_NOT_LOGGED_IN, err)
		}
		return response(pb.RpcBackupHealthExportNowResponseError_UNKNOWN_ERROR, err)
	}
	m := response(pb.RpcBackupHealthExportNowResponseError_NULL, nil)
	m.Summary = backupHealthExportSummaryToProto(svc.ExportAllSpacesNow(cctx, req.GetDestDir()))
	return m
}
