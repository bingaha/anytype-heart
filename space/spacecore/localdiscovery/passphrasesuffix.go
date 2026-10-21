package localdiscovery

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

// passphrasesuffix.go 是 08 号票的新增文件（加法）：整网口令只管"门"，
// 落到发现层就是"服务名后缀"。01 号票 localdiscovery/common.go 里的
// serviceName 常量原样不动——无口令时发现名就是它；设置口令后，实际广播
// 与浏览的名字是 EffectiveServiceName()，即 base + "-<8 hex>"。
//
// 后缀 = SHA256("myapp/discovery-name/v1:" || passphrase) 的前 8 个 hex
// 字符。base "_myapp"(6) + "-"(1) + 8 = 15 个字符，刚好卡在 DNS-SD
// service label 上限内。同一口令的设备互相可见；口令不同（或官方客户端）
// 的设备看到的是另一个服务名，浏览不到，也就不会建连。
//
// 口令来源由 SetDiscoveryPassphraseProvider 注入（08 号票的
// spacecore.SetWholeNetworkPassphrase 桥接，一处设置同时喂给 04 号票的
// admissionSaltFor 与这里）。localdiscovery 不直接依赖 spacecore，
// 避免 import cycle。

// discoveryNameDomain 是后缀哈希的 domain separation 前缀，与 04 号票的
// admissionSaltDomain、02 号票的 forkDiscoverySaltDomain 均不同。
const discoveryNameDomain = "myapp/discovery-name/v1:"

// discoverySuffixHexLen 是后缀取的 hex 字符数（见包注释的长度账）。
const discoverySuffixHexLen = 8

var (
	discoveryPassphraseMu       sync.RWMutex
	discoveryPassphraseProvider func() (string, error)
)

// SetDiscoveryPassphraseProvider 安装整网口令来源（生产：08 号票桥接后
// 的内存值，首次启动由前端经 keytar 读出后经 RPC 送入；单测：stub）。
// 传 nil 恢复为"无口令"（等价于环境/默认，fail-open 为 base 名）。
func SetDiscoveryPassphraseProvider(fn func() (string, error)) {
	discoveryPassphraseMu.Lock()
	defer discoveryPassphraseMu.Unlock()
	discoveryPassphraseProvider = fn
}

// discoveryPassphrase 返回当前整网口令；未配置或读失败返回 ""。
func discoveryPassphrase() string {
	discoveryPassphraseMu.RLock()
	fn := discoveryPassphraseProvider
	discoveryPassphraseMu.RUnlock()
	if fn == nil {
		return ""
	}
	pp, err := fn()
	if err != nil {
		return ""
	}
	return pp
}

// DiscoveryNameSuffix 返回当前口令对应的服务名后缀：无口令时为 ""，
// 有口令时为 "-<8 hex>"。纯函数（除读 provider 外无副作用），可单测。
func DiscoveryNameSuffix() string {
	pp := discoveryPassphrase()
	if pp == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(discoveryNameDomain + pp))
	return "-" + hex.EncodeToString(sum[:])[:discoverySuffixHexLen]
}

// EffectiveServiceName 返回实际用于 Register/Browse 的 mDNS 服务名：
// serviceName（01 号票常量，不动）+ DiscoveryNameSuffix()。
func EffectiveServiceName() string {
	return serviceName + DiscoveryNameSuffix()
}
