import { z } from 'zod'
import { parse as parseYaml } from 'yaml'
import pino from 'pino'
import { PaintBoardManager } from './paintboard'
import { type TokenRequest, PaintResultCode, type WebSocketData, type BanUidData, type QueryVisData, type QueryIpData } from './types'
import Bun from 'bun'
import workerpool from 'workerpool'

// Adding logger to the global scope
declare global {
    var logger: pino.Logger
    var pool: workerpool.Pool
}
const logger = pino({
    transport: {
        target: 'pino-pretty',
        options: {
            ignore: 'pid,hostname'
        }
    }
})
globalThis.logger = logger

const pool = workerpool.pool({
    workerType: 'web'
})
globalThis.pool = pool

const configSchema = z.strictObject({
    logLevel: z.enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal']),
    port: z.number(),
    paintDelay: z.number().min(0),
    useDB: z.boolean().default(false),
    width: z.number().min(1).default(1000),
    height: z.number().min(1).default(600),
    clearBoard: z.boolean().default(false),
    validationPaste: z.string().default('IkaPaintBoard'),
    key: z.string().optional(),
    cert: z.string().optional(),
    maxWebSocketPerIP: z.number().min(0).default(0),
    maxReadOnlyWebsocket: z.number().min(0).default(50),
    maxWriteOnlyWebsocket: z.number().min(0).default(5),
    banDuration: z.number().min(0).default(60000),
    ticksPerSecond: z.number().min(1).default(128),
    maxPacketPerSecond: z.number().min(1).default(128),
    // 🌟 CHANGE 1: Added maxPacketSize to the configuration schema.
    // The minimum is set to 1KB to avoid accidental lockouts with very small values.
    maxPacketSize: z.number().min(1024).default(32 * 1024),
    enableTokenCounting: z.boolean().default(false),
    maxAllowedUID: z.number().optional(),
    rootToken: z.string().optional(),
    activityStartTime: z.number().default(0),
    activityEndTime: z.number().default(1767196800000),
    allowQuery: z.boolean().default(false),
    enableBandwidthCounting: z.boolean().default(false)
})

let config: z.infer<typeof configSchema>
try {
    const configFile = await Bun.file('./config.yml').text()
    const parsedConfig = parseYaml(configFile)
    config = configSchema.parse(parsedConfig)
    logger.info({ config }, 'Config loaded')
} catch (error) {
    logger.error({ error }, 'Unable to load config')
    process.exit(1)
}

logger.level = config.logLevel

const colorHash = (id: number) => {
    // ANSI escape codes for some colors
    const colors = [
        '\x1b[31m', // red
        '\x1b[32m', // green
        '\x1b[33m', // yellow
        '\x1b[34m', // blue
        '\x1b[35m', // magenta
        '\x1b[36m', // cyan
        '\x1b[37m' // white
    ]
    const hash = id % colors.length
    const color = colors[hash]
    return `${color}#${id}\x1b[0m`
}

/**
 * Get the real client IP address from the request.
 * Prefers 'cf-connecting-ip' or 'x-forwarded-for' headers for scenarios behind a reverse proxy.
 * @param req - The incoming Request object.
 * @param fallbackAddr - The fallback IP address if the header is not present.
 * @returns The real client IP address.
 */
function getRealIp(req: Request, fallbackAddr?: string): string {
    const forwardedFor = req.headers.get('cf-connecting-ip') || req.headers.get('x-forwarded-for');
    if (forwardedFor) {
        return forwardedFor.split(',')[0].trim();
    }
    return fallbackAddr ?? 'unknown';
}

let webSocketConnectionCount = 0
const ipConnections = new Map<string, Bun.ServerWebSocket<WebSocketData>[]>()
const readOnlyIpConnections = new Map<string, Bun.ServerWebSocket<WebSocketData>[]>()
const writeOnlyIpConnections = new Map<string, Bun.ServerWebSocket<WebSocketData>[]>()
const bannedIPs = new Map<string, number>()
const bannedUIDs = new Set<number>()
const ipBandwidth = new Map<string, { sent: number, received: number }>()

function isBanned(ip: string): boolean {
    const banUntil = bannedIPs.get(ip)
    if (!banUntil) return false

    if (Date.now() >= banUntil) {
        bannedIPs.delete(ip)
        return false
    }
    return true
}

function banIP(ip: string) {
    bannedIPs.set(ip, Date.now() + config.banDuration)
    logger.warn(`IP ${ip} banned for ${config.banDuration}ms`)
}

function closeAllConnectionsForIP(ip: string, code: number, reason: string) {
    const regularConns = ipConnections.get(ip);
    if (regularConns) {
        for (const conn of regularConns) {
            conn.close(code, reason);
        }
        ipConnections.delete(ip);
    }
    const readOnlyConns = readOnlyIpConnections.get(ip);
    if (readOnlyConns) {
        for (const conn of readOnlyConns) {
            conn.close(code, reason);
        }
        readOnlyIpConnections.delete(ip);
    }
    const writeOnlyConns = writeOnlyIpConnections.get(ip);
    if (writeOnlyConns) {
        for (const conn of writeOnlyConns) {
            conn.close(code, reason);
        }
        writeOnlyIpConnections.delete(ip);
    }
}

let globalPacketsReceived = 0
let globalPacketsSent = 0
let lastTick = 0
let nextConnId = 1

const server = Bun.serve<WebSocketData>({
    static: {
        '/api': new Response(':(', {
            headers: {
                'Access-Control-Allow-Origin': '*'
            }
        }),
        '/dev/frontend': new Response(
            await Bun.file('./static/index.html').bytes(),
            {
                headers: {
                    'Content-Type': 'text/html',
                    'Access-Control-Allow-Origin': '*'
                }
            }
        )
    },
    fetch: async (req: Request, server) => {
        const fallbackIp = server.requestIP(req)?.address;
        const clientIp = getRealIp(req, fallbackIp);

        if (clientIp && isBanned(clientIp)) {
            return new Response('Too Many Requests', {
                status: 429,
                headers: {
                    'Access-Control-Allow-Origin': '*',
                    'Retry-After': Math.ceil(
                        (bannedIPs.get(clientIp)! - Date.now()) / 1000
                    ).toString()
                }
            })
        }

        if (req.method === 'OPTIONS') {
            return new Response(null, {
                headers: {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type'
                }
            })
        }

        const url = new URL(req.url)

        if (url.pathname === '/api/paintboard/ws') {
            const readonly = url.searchParams.get('readonly') === '1';
            const writeonly = url.searchParams.get('writeonly') === '1';

            if (
                server.upgrade(req, {
                    data: {
                        connectedAt: Date.now(),
                        ip: clientIp,
                        isInitialized: false,
                        readonly: readonly,
                        writeonly: writeonly
                    }
                })
            ) {
                return
            }
            return new Response('Upgrade failed', {
                status: 500,
                headers: {
                    'Access-Control-Allow-Origin': '*'
                }
            })
        }

        if (url.pathname === '/api/paintboard/getboard') {
            const startTime = Date.now()
            const [compressed, bufferSize] = await pool.exec<
                (
                    arg0: SharedArrayBuffer,
                    arg1: number,
                    arg2: number
                ) => [Uint8Array, number]
            >(
                (pixels: SharedArrayBuffer, width: number, height: number) => {
                    const gzipped = Bun.gzipSync(new Uint8Array(pixels), { level: 9 });
                    return [gzipped, width * height * 3]
                },
                [paintboard.getSharedArrayBuffer(), config.width, config.height]
            )
            logger.debug(
                `getboard: ${Date.now() - startTime}ms (gzip level 9) ${bufferSize} -> ${
                    compressed.length
                } (${(compressed.length / bufferSize).toFixed(2)}x)`
            )
            return new Response(compressed, {
                headers: {
                    'Content-Type': 'application/octet-stream',
                    'Access-Control-Allow-Origin': '*',
                    'Content-Encoding': 'gzip'
                }
            })
        }

        if (url.pathname === '/api/paintboard/getimage') {
            const startTime = Date.now()
            const [compressed, bufferSize] = await pool.exec<
                (
                    arg0: SharedArrayBuffer,
                    arg1: number,
                    arg2: number
                ) => Promise<[Buffer, number]>
            >(
                async (pixels: SharedArrayBuffer, width: number, height: number) => {
                    const sharp = await import('sharp')
                    const image = sharp.default(new Uint8Array(pixels), {
                        raw: {
                            width,
                            height,
                            channels: 3
                        }
                    })
                    const webpBuffer = await image.webp({ lossless: true, effort: 6 }).toBuffer()
                    return [webpBuffer, width * height * 3]
                },
                [paintboard.getSharedArrayBuffer(), config.width, config.height]
            )
            logger.debug(
                `getimage: ${
                    Date.now() - startTime
                }ms (webp-lossless effort 6) ${bufferSize} -> ${compressed.length} (${(
                    compressed.length / bufferSize
                ).toFixed(2)}x)`
            )
            return new Response(compressed, {
                headers: {
                    'Content-Type': 'image/webp',
                    'Access-Control-Allow-Origin': '*'
                }
            })
        }

        if (url.pathname === '/api/auth/gettoken' && req.method === 'POST') {
            return await handleTokenRequest(req)
        }

        if (url.pathname === '/api/root/banuid' && req.method === 'POST') {
            try {
                const body = (await req.json()) as BanUidData
                if (body.token !== config.rootToken) {
                    return new Response('Forbidden', {
                        status: 403,
                        headers: {
                            'Access-Control-Allow-Origin': '*'
                        }
                    })
                }
                bannedUIDs.add(body.uid)
                logger.info(`Banned UID ${body.uid}`)
                return new Response('OK', {
                    status: 200,
                    headers: {
                        'Access-Control-Allow-Origin': '*'
                    }
                })
            } catch (err) {
                return new Response('Bad Request', {
                    status: 400,
                    headers: {
                        'Access-Control-Allow-Origin': '*'
                    }
                })
            }
        }
        if (url.pathname === '/api/root/pardonuid' && req.method === 'POST') {
            try {
                const body = (await req.json()) as BanUidData
                if (body.token !== config.rootToken) {
                    return new Response('Forbidden', {
                        status: 403,
                        headers: {
                            'Access-Control-Allow-Origin': '*'
                        }
                    })
                }
                bannedUIDs.delete(body.uid)
                logger.info(`Pardoned UID ${body.uid}`)
                return new Response('OK', {
                    status: 200,
                    headers: {
                        'Access-Control-Allow-Origin': '*'
                    }
                })
            } catch (err) {
                return new Response('Bad Request', {
                    status: 400,
                    headers: {
                        'Access-Control-Allow-Origin': '*'
                    }
                })
            }
        }

        if (url.pathname === '/api/root/queryvis' && req.method === 'POST') {
            try {
                const body = (await req.json()) as QueryVisData
                if (body.token !== config.rootToken) {
                    return new Response('Forbidden', {
                        status: 403,
                        headers: {
                            'Access-Control-Allow-Origin': '*'
                        }
                    })
                }
                const vis = paintboard.getVis(body.x, body.y)
                return new Response(JSON.stringify({
                    statusCode: 200,
                    data: {
                        uid: vis.uid,
                        timestamp: vis.timestamp
                    }
                }), {
                    status: 200,
                    headers: {
                        'Access-Control-Allow-Origin': '*'
                    }
                })
            } catch (err) {
                return new Response('Bad Request', {
                    status: 400,
                    headers: {
                        'Access-Control-Allow-Origin': '*'
                    }
                })
            }
        }
        
        if (url.pathname === '/api/root/queryip' && req.method === 'POST') {
            try {
                const body = (await req.json()) as QueryIpData
                if (body.token !== config.rootToken) {
                    return new Response('Forbidden', {
                        status: 403,
                        headers: {
                            'Access-Control-Allow-Origin': '*'
                        }
                    })
                }

                const regularCount = ipConnections.get(body.ip)?.length ?? 0
                const readOnlyCount = readOnlyIpConnections.get(body.ip)?.length ?? 0
                const writeOnlyCount = writeOnlyIpConnections.get(body.ip)?.length ?? 0;

                return new Response(JSON.stringify({
                    statusCode: 200,
                    data: {
                        ip: body.ip,
                        regularConnections: regularCount,
                        readOnlyConnections: readOnlyCount,
                        writeOnlyConnections: writeOnlyCount
                    }
                }), {
                    status: 200,
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                })
            } catch (err) {
                return new Response('Bad Request', {
                    status: 400,
                    headers: {
                        'Access-Control-Allow-Origin': '*'
                    }
                })
            }
        }

        return new Response('Not Found', {
            status: 404,
            headers: {
                'Access-Control-Allow-Origin': '*'
            }
        })
    },
    idleTimeout: 120,
    websocket: {
        idleTimeout: 60,
        sendPings: false,
        publishToSelf: true,
        compression: 'SHARED_COMPRESSOR',
        open(ws) {
            const ip = ws.data.ip;
            if (!ip || ip === 'unknown') {
                ws.close(1011, 'Could not determine client IP address.');
                return;
            }

            if (isBanned(ip)) {
                ws.close(1008, 'IP is banned');
                return;
            }

            if (config.enableBandwidthCounting) {
                if (!ipBandwidth.has(ip)) {
                    ipBandwidth.set(ip, { sent: 0, received: 0 });
                }
            }

            if (ws.data.readonly) {
                let roConnections = readOnlyIpConnections.get(ip);
                if (!roConnections) {
                    roConnections = [];
                    readOnlyIpConnections.set(ip, roConnections);
                }

                if (config.maxReadOnlyWebsocket > 0 && roConnections.length >= config.maxReadOnlyWebsocket) {
                    logger.warn(`IP ${ip} exceeded read-only WebSocket limit, banning.`);
                    banIP(ip);
                    closeAllConnectionsForIP(ip, 1008, 'IP read-only connection limit exceeded');
                    return;
                }
                
                roConnections.push(ws);
                logger.debug(`Read-only WebSocket connected from ${ip}`);
                ws.subscribe('paint');
                return;
            }

            if (ws.data.writeonly) {
                let woConnections = writeOnlyIpConnections.get(ip);
                if (!woConnections) {
                    woConnections = [];
                    writeOnlyIpConnections.set(ip, woConnections);
                }
                
                if (config.maxWriteOnlyWebsocket > 0 && woConnections.length >= config.maxWriteOnlyWebsocket) {
                    logger.warn(`IP ${ip} exceeded write-only WebSocket limit, banning.`);
                    banIP(ip);
                    closeAllConnectionsForIP(ip, 1008, 'IP write-only connection limit exceeded');
                    return;
                }
                
                ws.data.isInitialized = true;
                ws.data.connId = nextConnId++;
                woConnections.push(ws);
                webSocketConnectionCount++;

                logger.debug(
                    `${colorHash(
                        ws.data.connId
                    )} ${ip} Write-Only WebSocket connected: ${webSocketConnectionCount} clients online`
                );

                ws.data.sendBuffer = new Bun.ArrayBufferSink();
                ws.data.sendBuffer.start({ asUint8Array: true, stream: true });

                ws.data.lastPing = Date.now();
                ws.data.waitingPong = false;
                ws.data.packetsReceived = 0;
                ws.data.lastPacketCountReset = Date.now();
                ws.data.nextPingDelay = Math.floor(Math.random() * 9000) + 1000;
                ws.data.pingTimer = setTimeout(() => sendPing(ws), ws.data.nextPingDelay);
                if (config.enableTokenCounting) {
                    ws.data.tokenUsageCount = new Set();
                }
                return;
            }
            
            let connections = ipConnections.get(ip)
            if (!connections) {
                connections = []
                ipConnections.set(ip, connections)
            }

            if (
                config.maxWebSocketPerIP > 0 &&
                connections.length >= config.maxWebSocketPerIP
            ) {
                logger.warn(`IP ${ip} exceeded WebSocket limit, banning.`);
                banIP(ip);
                closeAllConnectionsForIP(ip, 1008, 'IP connection limit exceeded');
                return;
            }

            ws.data.isInitialized = true;
            ws.data.connId = nextConnId++
            connections.push(ws)
            webSocketConnectionCount++

            logger.debug(
                `${colorHash(
                    ws.data.connId
                )} ${ip} WebSocket connected: ${webSocketConnectionCount} clients online`
            )

            ws.data.sendBuffer = new Bun.ArrayBufferSink()
            ws.data.sendBuffer.start({
                asUint8Array: true,
                stream: true
            })

            ws.subscribe('paint')

            ws.data.lastPing = Date.now()
            ws.data.waitingPong = false
            ws.data.packetsReceived = 0
            ws.data.lastPacketCountReset = Date.now()
            ws.data.nextPingDelay = Math.floor(Math.random() * 9000) + 1000
            ws.data.pingTimer = setTimeout(() => sendPing(ws), ws.data.nextPingDelay)
            if (config.enableTokenCounting) {
                ws.data.tokenUsageCount = new Set()
            }
        },
        close(ws) {
            const ip = ws.data.ip;

            if (ws.data.readonly) {
                logger.debug(`Read-only WebSocket from ${ip} disconnected.`);
                if (ip) {
                    const roConnections = readOnlyIpConnections.get(ip);
                    if (roConnections) {
                        const index = roConnections.indexOf(ws);
                        if (index > -1) {
                            roConnections.splice(index, 1);
                        }
                        if (roConnections.length === 0) {
                            readOnlyIpConnections.delete(ip);
                        }
                    }
                }
                return;
            }

            if (!ws.data.isInitialized) {
                return;
            }

            if (ws.data.pingTimer) {
                clearTimeout(ws.data.pingTimer);
            }
            if (ws.data.pongTimer) {
                clearTimeout(ws.data.pongTimer);
            }

            ws.data.sendBuffer?.flush();

            if (ws.data.writeonly) {
                if (ip) {
                    const woConnections = writeOnlyIpConnections.get(ip);
                    if (woConnections) {
                        const index = woConnections.indexOf(ws);
                        if (index > -1) {
                            woConnections.splice(index, 1);
                        }
                        if (woConnections.length === 0) {
                            writeOnlyIpConnections.delete(ip);
                        }
                    }
                }
            } else {
                if (ip) {
                    const connections = ipConnections.get(ip);
                    if (connections) {
                        const index = connections.indexOf(ws);
                        if (index > -1) {
                            connections.splice(index, 1);
                        }
                        if (connections.length === 0) {
                            ipConnections.delete(ip);
                        }
                    }
                }
            }
            
            webSocketConnectionCount--;

            logger.debug(
                `${colorHash(
                    ws.data.connId
                )} WebSocket closed: ${webSocketConnectionCount} clients remaining`
            );
        },
        message(ws, msg: Buffer) {
            if (ws.data.readonly) {
                logger.warn(`Read-only client from ${ws.data.ip} sent a packet. Disconnecting.`);
                ws.close(1008, 'Read-only clients cannot send data');
                return;
            }

            if (ws.readyState !== 1) return

            // 🌟 CHANGE 2: Enforce the maximum packet size limit.
            // This check is placed early to prevent processing of oversized messages.
            if (msg.length > config.maxPacketSize) {
                logger.warn(
                    `Client ${ws.data.ip} sent an oversized packet (${msg.length} > ${config.maxPacketSize} bytes). Disconnecting.`
                );
                // 1009 is the standard WebSocket close code for "Message Too Big"
                ws.close(1009, 'Message too big');
                return;
            }

            const now = Date.now()

            if (now - ws.data.lastPacketCountReset >= 1000) {
                ws.data.packetsReceived = 0
                ws.data.lastPacketCountReset = now
            }

            ws.data.packetsReceived++
            if (ws.data.packetsReceived > config.maxPacketPerSecond) {
                const ip = ws.data.ip!
                logger.warn(
                    `Client ${ip} exceeded packet rate limit (${ws.data.packetsReceived} > ${config.maxPacketPerSecond}), banning for 15s`
                )
                bannedIPs.set(ip, Date.now() + 15000)
                closeAllConnectionsForIP(ip, 1013, 'Packet rate limit exceeded');
                return
            }

            globalPacketsReceived++

            if (config.enableBandwidthCounting) {
                const ip = ws.data.ip!;
                const bandwidth = ipBandwidth.get(ip) ?? { sent: 0, received: 0 };
                bandwidth.received += msg.length;
                ipBandwidth.set(ip, bandwidth);
            }

            try {
                const dataView = new DataView(msg.buffer)
                let offset = 0

                while (offset < msg.length) {
                    const type = dataView.getUint8(offset)
                    offset += 1

                    switch (type) {
                        case 0xfb: // C2S pong
                            if (!ws.data.waitingPong) {
                                logger.warn(
                                    `${colorHash(ws.data.connId)} Received unexpected pong from ${
                                        ws.data.ip
                                    }`
                                )
                                ws.close(1002, 'Protocol violation: unexpected pong')
                                return
                            }

                            if (ws.data.pongTimer) {
                                clearTimeout(ws.data.pongTimer)
                                ws.data.pongTimer = undefined
                            }

                            ws.data.waitingPong = false
                            ws.data.lastPing = Date.now()

                            ws.data.nextPingDelay = Math.floor(Math.random() * 9000) + 1000
                            ws.data.pingTimer = setTimeout(
                                () => sendPing(ws),
                                ws.data.nextPingDelay
                            )
                            break

                        case 0xfe: {
                            const x = dataView.getUint16(offset, true)
                            const y = dataView.getUint16(offset + 2, true)
                            const color = {
                                r: dataView.getUint8(offset + 4),
                                g: dataView.getUint8(offset + 5),
                                b: dataView.getUint8(offset + 6)
                            }
                            const uid =
                                dataView.getUint8(offset + 7) +
                                dataView.getUint8(offset + 8) * 256 +
                                dataView.getUint8(offset + 9) * 65536

                            const tokenBytes = new Uint8Array(msg.buffer, offset + 10, 16)
                            const token = [
                                Buffer.from(tokenBytes.slice(0, 4)).toString('hex'),
                                Buffer.from(tokenBytes.slice(4, 6)).toString('hex'),
                                Buffer.from(tokenBytes.slice(6, 8)).toString('hex'),
                                Buffer.from(tokenBytes.slice(8, 10)).toString('hex'),
                                Buffer.from(tokenBytes.slice(10, 16)).toString('hex')
                            ].join('-')

                            const id = dataView.getUint32(offset + 26, true)
                            offset += 30

                            if (config.enableTokenCounting) {
                                ws.data.tokenUsageCount.add(token)
                            }
                            let result = 0x00
                            if (bannedUIDs.has(uid))
                            {
                                result = PaintResultCode.NO_PERMISSION
                            }
                            else
                            {
                                if(Date.now() > config.activityEndTime || Date.now() < config.activityStartTime) {
                                    logger.info('Painting before activity started or after ended, terminating connection')
                                    ws.close(1003, 'Activity not started or already ended')
                                    return
                                }

                                result = paintboard.validateToken(token, uid)
                                if (result === PaintResultCode.SUCCESS) {
                                    const success = paintboard.setPixel(x, y, color, uid)
                                    if (!success) {
                                        result = PaintResultCode.BAD_FORMAT
                                    }
                                }
                            }

                            const response = new Uint8Array([
                                0xff,
                                id & 255,
                                (id >> 8) & 255,
                                (id >> 16) & 255,
                                (id >> 24) & 255,
                                result
                            ])
                            ws.data.sendBuffer.write(response)
                            break
                        }

                        default:
                            logger.warn(
                                `${colorHash(ws.data.connId)} Unknown packet type: ${type}`
                            )
                            ws.close(1002, 'Protocol violation: unknown packet type')
                            return
                    }
                }
            } catch (e) {
                logger.error(e, 'Error processing message, terminating connection')
                ws.close(1011, 'Server error processing message')
            }
        }
    },

    port: config.port,
    ...(config.key && config.cert
        ? {
            tls: {
                key: Bun.file(config.key),
                cert: Bun.file(config.cert)
            }
        }
        : {})
})

const paintboard = new PaintBoardManager(
    config.width,
    config.height,
    config.paintDelay,
    config.validationPaste,
    config.useDB,
    config.clearBoard,
    config.allowQuery
)

paintboard.onColorUpdate(batchUpdate => {
    const sent = server.publish('paint', batchUpdate, true) 
    if (sent > 0) {
        globalPacketsSent += (ipConnections.size + readOnlyIpConnections.size);

        if (config.enableBandwidthCounting) {
            const updateSize = (batchUpdate as Buffer).length;
            const subscribedIps = new Set([...ipConnections.keys(), ...readOnlyIpConnections.keys()]);

            for (const ip of subscribedIps) {
                const regularCount = ipConnections.get(ip)?.length ?? 0;
                const readOnlyCount = readOnlyIpConnections.get(ip)?.length ?? 0;
                const totalConnections = regularCount + readOnlyCount;

                if (totalConnections > 0) {
                    const bandwidth = ipBandwidth.get(ip) ?? { sent: 0, received: 0 };
                    bandwidth.sent += totalConnections * updateSize;
                    ipBandwidth.set(ip, bandwidth);
                }
            }
        }
    }
})

setInterval(() => {
    const now = Date.now()
    const elapsed = now - lastTick
    if (lastTick && elapsed > 1000 / config.ticksPerSecond + 50)
        logger.warn(
            `Can't keep up! Is the server overloaded? Last tick took ${elapsed}ms!`
        )
    lastTick = now

    const flushBuffersForConnections = (connections: Bun.ServerWebSocket<WebSocketData>[], ip: string) => {
        for (const ws of connections) {
            const buffer = ws.data.sendBuffer.flush() as Uint8Array;
            if (buffer.length > 0) {
                ws.send(buffer);
                if (config.enableBandwidthCounting) {
                    const bandwidth = ipBandwidth.get(ip) ?? { sent: 0, received: 0 };
                    bandwidth.sent += buffer.length;
                    ipBandwidth.set(ip, bandwidth);
                }
            }
        }
    };

    for (const [ip, connections] of ipConnections) {
        flushBuffersForConnections(connections, ip);
    }
    
    for (const [ip, connections] of writeOnlyIpConnections) {
        flushBuffersForConnections(connections, ip);
    }

    paintboard.flushUpdates()
}, 1000 / config.ticksPerSecond)

setInterval(() => {
    let statsMessage = `WebSocket Traffic - Received: ${globalPacketsReceived} packets (${(
        globalPacketsReceived / 5
    ).toFixed(2)} /s), Sent: ${globalPacketsSent} packets (${(
        globalPacketsSent / 5
    ).toFixed(2)} /s)`

    if (config.enableBandwidthCounting) {
        const totalBandwidth = Array.from(ipBandwidth.values()).reduce(
            (acc, curr) => {
                acc.sent += curr.sent;
                acc.received += curr.received;
                return acc;
            },
            { sent: 0, received: 0 }
        );

        statsMessage += `\nTotal Bandwidth - Down: ${(totalBandwidth.sent / 1024 / 5).toFixed(2)} KB/s, Up: ${(totalBandwidth.received / 1024 / 5).toFixed(2)} KB/s`;

        const top5Bandwidth = Array.from(ipBandwidth.entries())
            .sort(([, a], [, b]) => (b.sent + b.received) - (a.sent + a.received))
            .slice(0, 5);

        if (top5Bandwidth.length > 0) {
            statsMessage += `\nTop 5 Bandwidth Users (Client Down | Up) [KB/s]:\n${top5Bandwidth
                .map(([ip, bw]) => `  ${ip}: ${(bw.sent / 1024 / 5).toFixed(2)} | ${(bw.received / 1024 / 5).toFixed(2)}`)
                .join('\n')}`;
        }
    }

    if (config.enableTokenCounting) {
        const allConnections = [...Array.from(ipConnections.values()).flat(), ...Array.from(writeOnlyIpConnections.values()).flat()];
        const connectionStats = allConnections
            .map(ws => ({
                ip: ws.data.ip,
                uniqueTokens: ws.data.tokenUsageCount.size
            }))
            .sort((a, b) => b.uniqueTokens - a.uniqueTokens)

        const top5 = connectionStats.slice(0, 5)
        statsMessage += `\nTop 5 Token Users:\n${top5
            .map(stat => `  ${stat.ip}: ${stat.uniqueTokens} tokens`)
            .join('\n')}`
    }

    logger.info(statsMessage)

    globalPacketsReceived = 0
    globalPacketsSent = 0
    if (config.enableBandwidthCounting) {
        ipBandwidth.clear();
    }
}, 5000)

function handleShutdown() {
    logger.info('Server shutting down...')
    paintboard.shutdown()
    process.exit(0)
}

process.on('SIGINT', handleShutdown)
process.on('SIGTERM', handleShutdown)

async function handleTokenRequest(req: Request): Promise<Response> {
    try {
        const body = (await req.json()) as TokenRequest

        if (!Number.isInteger(body.uid)) {
            return new Response(
                JSON.stringify({
                    statusCode: 400,
                    data: {
                        errorType: 'BAD_REQUEST',
                        message: 'Invalid request format'
                    }
                }),
                {
                    status: 400,
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                }
            )
        }

        if (config.maxAllowedUID && body.uid > config.maxAllowedUID) {
            return new Response(
                JSON.stringify({
                    statusCode: 403,
                    data: {
                        errorType: 'UID_NOT_ALLOWED',
                        message: `UID must be less than or equal to ${config.maxAllowedUID}`
                    }
                }),
                {
                    status: 403,
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                }
            )
        }

        const result = await paintboard.generateToken(body.uid, (body as any).access_key)

        if (!result.token) {
            if (
                result.error === 'INVALID_ACCESS_KEY' ||
                result.error === 'UID_MISMATCH' ||
                result.error === 'CONTENT_MISMATCH'
            ) {
                return new Response(
                    JSON.stringify({
                        statusCode: 403,
                        data: {
                            errorType: result.error
                        }
                    }),
                    {
                        status: 403,
                        headers: {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*'
                        }
                    }
                )
            }

            return new Response(
                JSON.stringify({
                    statusCode: 500,
                    data: {
                        errorType: 'SERVER_ERROR'
                    }
                }),
                {
                    status: 500,
                    headers: {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                }
            )
        }

        return new Response(
            JSON.stringify({
                statusCode: 200,
                data: {
                    token: result.token
                }
            }),
            {
                status: 200,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }
        )
    } catch (e) {
        logger.error(e, 'Failed to parse token request')
        return new Response(
            JSON.stringify({
                statusCode: 400,
                data: {
                    errorType: 'BAD_REQUEST',
                    message: 'Invalid request format'
                }
            }),
            {
                status: 400,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }
        )
    }
}

logger.info(`Server started on port ${config.port}`)

function sendPing(ws: Bun.ServerWebSocket<WebSocketData>) {
    if (ws.data.waitingPong) {
        ws.close(1002, 'Protocol violation: duplicate ping state')
        return
    }

    ws.data.waitingPong = true
    ws.data.sendBuffer.write(new Uint8Array([0xfc]))

    ws.data.pongTimer = setTimeout(() => {
        if (ws.data.waitingPong) {
            logger.debug(
                `${colorHash(ws.data.connId)} WebSocket ping timeout for ${ws.data.ip}`
            )
            ws.close(1001, 'Ping timeout')
        }
    }, 3000)
}