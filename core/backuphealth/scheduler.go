package backuphealth

// 定期导出调度：ticker 触发 ExportAllSpacesMarkdown，前一次没跑完就跳过。
// 默认关闭，由设置页开关（08 号按钮）调用 SetPeriodicExport 打开。

import (
	"context"
	"sync"
	"time"
)

type periodicExport struct {
	mu       sync.Mutex
	cancel   context.CancelFunc
	running  bool
	interval time.Duration
	destDir  string
}

// set 启动或替换定期导出；interval<=0 视为停止。
func (p *periodicExport) set(svc *service, interval time.Duration, destDir string) {
	p.mu.Lock()
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	p.interval = interval
	p.destDir = destDir
	if interval <= 0 || destDir == "" {
		p.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.mu.Unlock()
	go p.loop(ctx, svc)
}

func (p *periodicExport) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
}

func (p *periodicExport) loop(ctx context.Context, svc *service) {
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.mu.Lock()
			if p.running {
				p.mu.Unlock()
				continue
			}
			p.running = true
			p.mu.Unlock()
			func() {
				defer func() {
					p.mu.Lock()
					p.running = false
					p.mu.Unlock()
				}()
				ectx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
				defer cancel()
				svc.ExportAllSpacesNow(ectx, p.destDir)
			}()
		}
	}
}
