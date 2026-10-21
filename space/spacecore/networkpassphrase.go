package spacecore

import (
	"github.com/anyproto/anytype-heart/space/spacecore/localdiscovery"
)

// networkpassphrase.go 是 08 号票的新增文件（加法）：整网口令在运行时的
// 唯一入口。一处设置，同时喂给两道"门"——
//
//  1. 握手加盐：04 号票的 admissionSaltFor 经 networkPassphrase() 读口令，
//     来源是 SetNetworkPassphraseProvider。这里装一个返回内存值的 provider。
//  2. 发现后缀：01/08 号票的 localdiscovery.EffectiveServiceName() 经
//     discoveryPassphraseProvider 读口令，这里同样装一个内存值。
//
// 口令本身的持久化不在后端：前端首次启动输入后经 Electron keytar/
// safeStorage 通道存入系统钥匙串，每次启动由前端读出后经
// NetworkPassphraseSet RPC 送入此处。后端不落盘口令（盘上保持明文的是
// 数据，不是开门的钥匙）。
//
// 生效时机：发现名在 Register/Browse 启动时求值一次。运行中设置新口令会
// 立刻改变后续 EffectiveServiceName() 的返回值，但已启动的 mDNS
// server/browser 仍用旧名——重启后新名生效（验收流程即"设置→重启→
// 钥匙串有值→发现名带后缀"）。握手加盐是每次握手现取，无需重启。

// SetWholeNetworkPassphrase 安装整网口令（内存值）。空字符串表示"未组网"：
// 握手门 fail-closed（04 号票 errAdmissionNoPassphrase），发现退回 base 名。
func SetWholeNetworkPassphrase(passphrase string) {
	SetNetworkPassphraseProvider(func() (string, error) {
		return passphrase, nil
	})
	localdiscovery.SetDiscoveryPassphraseProvider(func() (string, error) {
		return passphrase, nil
	})
}

// ClearWholeNetworkPassphrase 恢复为无 provider 状态（回退到
// MYAPP_NETWORK_PASSPHARSE 环境变量，行为与 04 号票默认一致）。
func ClearWholeNetworkPassphrase() {
	SetNetworkPassphraseProvider(nil)
	localdiscovery.SetDiscoveryPassphraseProvider(nil)
}

// WholeNetworkPassphraseConfigured 报告当前是否装有可用口令（只返回有无，
// 不返回口令本身，供 NetworkPassphraseStatus RPC 与设置页展示用）。
func WholeNetworkPassphraseConfigured() bool {
	_, err := networkPassphrase()
	return err == nil
}

// DiscoveryServiceName 返回当前实际用于 mDNS Register/Browse 的服务名
// （base + 口令后缀），供 RPC 回显与日志验证。
func DiscoveryServiceName() string {
	return localdiscovery.EffectiveServiceName()
}
