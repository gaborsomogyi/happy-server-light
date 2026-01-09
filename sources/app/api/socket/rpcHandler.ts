import { buildSessionActivityEphemeral, eventRouter } from "@/app/events/eventRouter";
import { db } from "@/storage/db";
import { log } from "@/utils/log";
import { Socket } from "socket.io";

export function rpcHandler(userId: string, socket: Socket, rpcListeners: Map<string, Socket>) {
    
    // RPC register - Register this socket as a listener for an RPC method
    socket.on('rpc-register', async (data: any) => {
        try {
            const { method } = data;

            if (!method || typeof method !== 'string') {
                socket.emit('rpc-error', { type: 'register', error: 'Invalid method name' });
                return;
            }

            // Check if method was already registered
            const previousSocket = rpcListeners.get(method);
            if (previousSocket && previousSocket !== socket) {
                // log({ module: 'websocket-rpc' }, `RPC method ${method} re-registered: ${previousSocket.id} -> ${socket.id}`);
            }

            // Register this socket as the listener for this method
            rpcListeners.set(method, socket);

            socket.emit('rpc-registered', { method });
            // log({ module: 'websocket-rpc' }, `RPC method registered: ${method} on socket ${socket.id} (user: ${userId})`);
            // log({ module: 'websocket-rpc' }, `Active RPC methods for user ${userId}: ${Array.from(rpcListeners.keys()).join(', ')}`);
        } catch (error) {
            log({ module: 'websocket', level: 'error' }, `Error in rpc-register: ${error}`);
            socket.emit('rpc-error', { type: 'register', error: 'Internal error' });
        }
    });

    // RPC unregister - Remove this socket as a listener for an RPC method
    socket.on('rpc-unregister', async (data: any) => {
        try {
            const { method } = data;

            if (!method || typeof method !== 'string') {
                socket.emit('rpc-error', { type: 'unregister', error: 'Invalid method name' });
                return;
            }

            if (rpcListeners.get(method) === socket) {
                rpcListeners.delete(method);
                // log({ module: 'websocket-rpc' }, `RPC method unregistered: ${method} from socket ${socket.id} (user: ${userId})`);

                if (rpcListeners.size === 0) {
                    rpcListeners.delete(userId);
                    // log({ module: 'websocket-rpc' }, `All RPC methods unregistered for user ${userId}`);
                } else {
                    // log({ module: 'websocket-rpc' }, `Remaining RPC methods for user ${userId}: ${Array.from(rpcListeners.keys()).join(', ')}`);
                }
            } else {
                // log({ module: 'websocket-rpc' }, `RPC unregister ignored: ${method} not registered on socket ${socket.id}`);
            }

            socket.emit('rpc-unregistered', { method });
        } catch (error) {
            log({ module: 'websocket', level: 'error' }, `Error in rpc-unregister: ${error}`);
            socket.emit('rpc-error', { type: 'unregister', error: 'Internal error' });
        }
    });

    // RPC call - Call an RPC method on another socket of the same user
    socket.on('rpc-call', async (data: any, callback: (response: any) => void) => {
        try {
            const { method, params } = data;

            if (!method || typeof method !== 'string') {
                if (callback) {
                    callback({
                        ok: false,
                        error: 'Invalid parameters: method is required'
                    });
                }
                return;
            }

            const targetSocket = rpcListeners.get(method);
            if (!targetSocket || !targetSocket.connected) {
                // log({ module: 'websocket-rpc' }, `RPC call failed: Method ${method} not available (disconnected or not registered)`);
                if (callback) {
                    callback({
                        ok: false,
                        error: 'RPC method not available'
                    });
                }
                return;
            }

            // Don't allow calling your own socket
            if (targetSocket === socket) {
                // log({ module: 'websocket-rpc' }, `RPC call failed: Attempted self-call on method ${method}`);
                if (callback) {
                    callback({
                        ok: false,
                        error: 'Cannot call RPC on the same socket'
                    });
                }
                return;
            }

            // Log RPC call initiation
            const startTime = Date.now();
            // log({ module: 'websocket-rpc' }, `RPC call initiated: ${socket.id} -> ${method} (target: ${targetSocket.id})`);

            // Forward the RPC request to the target socket using emitWithAck
            try {
                const response = await targetSocket.timeout(30000).emitWithAck('rpc-request', {
                    method,
                    params
                });

                const duration = Date.now() - startTime;
                // log({ module: 'websocket-rpc' }, `RPC call succeeded: ${method} (${duration}ms)`);

                // Forward the response back to the caller via callback
                if (callback) {
                    callback({
                        ok: true,
                        result: response
                    });
                }

            } catch (error) {
                const duration = Date.now() - startTime;
                const errorMsg = error instanceof Error ? error.message : 'RPC call failed';
                // log({ module: 'websocket-rpc' }, `RPC call failed: ${method} - ${errorMsg} (${duration}ms)`);

                // Timeout or error occurred
                if (callback) {
                    callback({
                        ok: false,
                        error: errorMsg
                    });
                }
            }
        } catch (error) {
            // log({ module: 'websocket', level: 'error' }, `Error in rpc-call: ${error}`);
            if (callback) {
                callback({
                    ok: false,
                    error: 'Internal error'
                });
            }
        }
    });

    socket.on('disconnect', async () => {

        const methodsToRemove: string[] = [];
        for (const [method, registeredSocket] of rpcListeners.entries()) {
            if (registeredSocket === socket) {
                methodsToRemove.push(method);
            }
        }

        if (methodsToRemove.length > 0) {
            // log({ module: 'websocket-rpc' }, `Cleaning up RPC methods on disconnect for socket ${socket.id}: ${methodsToRemove.join(', ')}`);
            methodsToRemove.forEach(method => rpcListeners.delete(method));
        }

        // If this disconnect removed any session RPC methods, proactively mark those sessions inactive.
        // This prevents "ghost online" sessions where the DB still says active=true but the RPC listener is gone.
        try {
            const candidateSessionIds = Array.from(new Set(
                methodsToRemove
                    .map((method) => method.split(':')[0])
                    .filter((id) => Boolean(id))
            ));

            if (candidateSessionIds.length > 0) {
                const now = Date.now();

                // Only update sessions that actually exist for this user and are currently active.
                const activeSessions = await db.session.findMany({
                    where: {
                        accountId: userId,
                        id: { in: candidateSessionIds },
                        active: true,
                    },
                    select: { id: true },
                });

                if (activeSessions.length > 0) {
                    const sessionIds = activeSessions.map((s) => s.id);

                    await db.session.updateMany({
                        where: { accountId: userId, id: { in: sessionIds }, active: true },
                        data: { active: false, lastActiveAt: new Date(now) },
                    });

                    for (const sessionId of sessionIds) {
                        eventRouter.emitEphemeral({
                            userId,
                            payload: buildSessionActivityEphemeral(sessionId, false, now, false),
                            recipientFilter: { type: 'user-scoped-only' },
                        });
                    }
                }
            }
        } catch (error) {
            log({ module: 'websocket', level: 'error' }, `Error marking sessions inactive on disconnect: ${error}`);
        }

        if (rpcListeners.size === 0) {
            rpcListeners.delete(userId);
            // log({ module: 'websocket-rpc' }, `All RPC listeners removed for user ${userId}`);
        }
    });
}