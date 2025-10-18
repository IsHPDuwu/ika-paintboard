import type Bun from 'bun'
import type { Timer } from 'bun'

/**
 * 表示一个 RGB 颜色
 */
export type Color = {
    r: number
    g: number
    b: number
}

/**
 * 描述画板的核心数据结构
 */
export type PaintBoard = {
    width: number
    height: number
    pixels: SharedArrayBuffer // 使用 SharedArrayBuffer 以在多线程/worker之间共享像素数据
}

/**
 * 表示一个生成的、用于绘制的令牌
 */
export type Token = {
    uid: number
    token: string
}

/**
 * 存储在画板上每个像素点的归属和时间信息
 */
export type PixelData = {
    uid: number
    timestamp: number
}

/**
 * 绘制操作返回的结果码枚举
 */
export enum PaintResultCode {
    SUCCESS = 0xef,       // 成功
    INVALID_TOKEN = 0xed, // Token 无效或过期
    COOLING = 0xee,       // 冷却中
    BAD_FORMAT = 0xec,    // 坐标或颜色格式错误
    NO_PERMISSION = 0xeb, // 无权限（例如UID被封禁）
    SERVER_ERROR = 0xea    // 服务器内部错误
}

/**
 * 客户端请求生成 Token 时发送的 Body 结构
 * 修正：根据 index.ts 中的使用，将 `paste` 字段改为 `access_key`
 */
export type TokenRequest = {
    uid: number
    access_key: string
}

/**
 * 管理员用于封禁或解封特定 UID 的请求 Body 结构
 * 修正：根据 index.ts 中的使用，移除了未使用的 `time` 字段
 */
export type BanUidData = {
    token: string // 管理员 rootToken
    uid: number
}

/**
 * 管理员用于查询特定坐标点绘制信息的请求 Body 结构
 */
export type QueryVisData = {
    token: string // 管理员 rootToken
    x: number
    y: number
}

export type FillData = {
	token: string
	x0: number
	y0: number
	x1: number
	y1: number
	color: string
}

/**
 * 新增：管理员用于查询特定 IP 连接信息的请求 Body 结构
 */
export type QueryIpData = {
    token: string // 管理员 rootToken
    ip: string
}

/**
 * 附加到每个 WebSocket 连接上的自定义数据对象
 */
export type WebSocketData = {
    connId: number          // 服务器分配的唯一连接 ID
    connectedAt: number     // 连接建立的时间戳
    ip: string              // 客户端的真实 IP 地址

    // ---- 状态与限制 ----
    isInitialized: boolean  // 连接是否已成功通过所有检查并初始化
    readonly: boolean       // 是否为只读连接
    packetsReceived: number // 在当前时间窗口内收到的包数量
    lastPacketCountReset: number // 上次重置包计数器的时间戳
    tokenUsageCount: Set<string> // 在此连接上使用过的不同 token 的集合

    // ---- 心跳与保活 ----
    lastPing: number        // 上次收到 pong 或成功通信的时间戳
    waitingPong: boolean    // 是否正在等待客户端的 pong 响应
    pingTimer?: Timer       // 用于发送下一次 ping 的 setTimeout 定时器
    pongTimer?: Timer       // 用于检测 pong 是否超时的 setTimeout 定时器
    nextPingDelay?: number  // 下一次 ping 的动态延迟时间(ms)

    // ---- 数据发送 ----
    sendBuffer: Bun.ArrayBufferSink // 用于批量发送数据的缓冲区

    // ---- 可选的用户信息 (如果需要登录功能) ----
    uid?: number
    token?: string
}

/**
 * 颜色更新事件监听器的函数签名
 */
export type ColorUpdateListener = (batchUpdate: Uint8Array) => void