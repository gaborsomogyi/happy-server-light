import { sessionAliveEventsCounter, websocketEventsCounter } from "@/app/monitoring/metrics2";
import { activityCache } from "@/app/presence/sessionCache";
import { buildNewMessageUpdate, buildPendingQueueEphemeral, buildSessionActivityEphemeral, buildUpdateSessionUpdate, ClientConnection, eventRouter } from "@/app/events/eventRouter";
import { db } from "@/storage/db";
import { allocateSessionSeq, allocateUserSeq } from "@/storage/seq";
import { AsyncLock } from "@/utils/lock";
import { log } from "@/utils/log";
import { randomKeyNaked } from "@/utils/randomKeyNaked";
import { Socket } from "socket.io";

export function sessionUpdateHandler(userId: string, socket: Socket, connection: ClientConnection) {
    socket.on('update-metadata', async (data: any, callback: (response: any) => void) => {
        try {
            const { sid, metadata, expectedVersion } = data;

            // Validate input
            if (!sid || typeof metadata !== 'string' || typeof expectedVersion !== 'number') {
                if (callback) {
                    callback({ result: 'error' });
                }
                return;
            }

            // Resolve session
            const session = await db.session.findUnique({
                where: { id: sid, accountId: userId }
            });
            if (!session) {
                return;
            }

            // Check version
            if (session.metadataVersion !== expectedVersion) {
                callback({ result: 'version-mismatch', version: session.metadataVersion, metadata: session.metadata });
                return null;
            }

            // Update metadata
            const { count } = await db.session.updateMany({
                where: { id: sid, metadataVersion: expectedVersion },
                data: {
                    metadata: metadata,
                    metadataVersion: expectedVersion + 1
                }
            });
            if (count === 0) {
                callback({ result: 'version-mismatch', version: session.metadataVersion, metadata: session.metadata });
                return null;
            }

            // Generate session metadata update
            const updSeq = await allocateUserSeq(userId);
            const metadataUpdate = {
                value: metadata,
                version: expectedVersion + 1
            };
            const updatePayload = buildUpdateSessionUpdate(sid, updSeq, randomKeyNaked(12), metadataUpdate);
            eventRouter.emitUpdate({
                userId,
                payload: updatePayload,
                recipientFilter: { type: 'all-interested-in-session', sessionId: sid }
            });

            // Send success response with new version via callback
            callback({ result: 'success', version: expectedVersion + 1, metadata: metadata });
        } catch (error) {
            log({ module: 'websocket', level: 'error' }, `Error in update-metadata: ${error}`);
            if (callback) {
                callback({ result: 'error' });
            }
        }
    });

    socket.on('update-state', async (data: any, callback: (response: any) => void) => {
        try {
            const { sid, agentState, expectedVersion } = data;

            // Validate input
            if (!sid || (typeof agentState !== 'string' && agentState !== null) || typeof expectedVersion !== 'number') {
                if (callback) {
                    callback({ result: 'error' });
                }
                return;
            }

            // Resolve session
            const session = await db.session.findUnique({
                where: {
                    id: sid,
                    accountId: userId
                }
            });
            if (!session) {
                callback({ result: 'error' });
                return null;
            }

            // Check version
            if (session.agentStateVersion !== expectedVersion) {
                callback({ result: 'version-mismatch', version: session.agentStateVersion, agentState: session.agentState });
                return null;
            }

            // Update agent state
            const { count } = await db.session.updateMany({
                where: { id: sid, agentStateVersion: expectedVersion },
                data: {
                    agentState: agentState,
                    agentStateVersion: expectedVersion + 1
                }
            });
            if (count === 0) {
                callback({ result: 'version-mismatch', version: session.agentStateVersion, agentState: session.agentState });
                return null;
            }

            // Generate session agent state update
            const updSeq = await allocateUserSeq(userId);
            const agentStateUpdate = {
                value: agentState,
                version: expectedVersion + 1
            };
            const updatePayload = buildUpdateSessionUpdate(sid, updSeq, randomKeyNaked(12), undefined, agentStateUpdate);
            eventRouter.emitUpdate({
                userId,
                payload: updatePayload,
                recipientFilter: { type: 'all-interested-in-session', sessionId: sid }
            });

            // Send success response with new version via callback
            callback({ result: 'success', version: expectedVersion + 1, agentState: agentState });
        } catch (error) {
            log({ module: 'websocket', level: 'error' }, `Error in update-state: ${error}`);
            if (callback) {
                callback({ result: 'error' });
            }
        }
    });
    socket.on('session-alive', async (data: {
        sid: string;
        time: number;
        thinking?: boolean;
    }) => {
        try {
            // Track metrics
            websocketEventsCounter.inc({ event_type: 'session-alive' });
            sessionAliveEventsCounter.inc();

            // Basic validation
            if (!data || typeof data.time !== 'number' || !data.sid) {
                return;
            }

            let t = data.time;
            if (t > Date.now()) {
                t = Date.now();
            }
            if (t < Date.now() - 1000 * 60 * 10) {
                return;
            }

            const { sid, thinking } = data;

            // Check session validity using cache
            const isValid = await activityCache.isSessionValid(sid, userId);
            if (!isValid) {
                return;
            }

            // Queue database update (will only update if time difference is significant)
            activityCache.queueSessionUpdate(sid, t);

            // Emit session activity update
            const sessionActivity = buildSessionActivityEphemeral(sid, true, t, thinking || false);
            eventRouter.emitEphemeral({
                userId,
                payload: sessionActivity,
                recipientFilter: { type: 'user-scoped-only' }
            });
        } catch (error) {
            log({ module: 'websocket', level: 'error' }, `Error in session-alive: ${error}`);
        }
    });

    const receiveMessageLock = new AsyncLock();
    const pendingQueueLock = new AsyncLock();
    socket.on('message', async (data: any) => {
        await receiveMessageLock.inLock(async () => {
            try {
                websocketEventsCounter.inc({ event_type: 'message' });
                const { sid, message, localId } = data;

                log({ module: 'websocket' }, `Received message from socket ${socket.id}: sessionId=${sid}, messageLength=${message.length} bytes, connectionType=${connection.connectionType}, connectionSessionId=${connection.connectionType === 'session-scoped' ? connection.sessionId : 'N/A'}`);

                // Resolve session
                const session = await db.session.findUnique({
                    where: { id: sid, accountId: userId }
                });
                if (!session) {
                    return;
                }
                let useLocalId = typeof localId === 'string' ? localId : null;

                // Create encrypted message
                const msgContent: PrismaJson.SessionMessageContent = {
                    t: 'encrypted',
                    c: message
                };

                // Resolve seq
                const updSeq = await allocateUserSeq(userId);
                const msgSeq = await allocateSessionSeq(sid);

                // Check if message already exists
                if (useLocalId) {
                    const existing = await db.sessionMessage.findFirst({
                        where: { sessionId: sid, localId: useLocalId }
                    });
                    if (existing) {
                        return { msg: existing, update: null };
                    }
                }

                // Create message
                const msg = await db.sessionMessage.create({
                    data: {
                        sessionId: sid,
                        seq: msgSeq,
                        content: msgContent,
                        localId: useLocalId
                    }
                });

                // Emit new message update to relevant clients
                const updatePayload = buildNewMessageUpdate(msg, sid, updSeq, randomKeyNaked(12));
                eventRouter.emitUpdate({
                    userId,
                    payload: updatePayload,
                    recipientFilter: { type: 'all-interested-in-session', sessionId: sid },
                    skipSenderConnection: connection
                });
            } catch (error) {
                log({ module: 'websocket', level: 'error' }, `Error in message handler: ${error}`);
            }
        });
    });

    socket.on('pending-enqueue', async (data: any, callback: (response: any) => void) => {
        await pendingQueueLock.inLock(async () => {
            try {
                websocketEventsCounter.inc({ event_type: 'pending-enqueue' });
                const { sid, message, localId } = data ?? {};

                if (!sid || typeof sid !== 'string' || typeof message !== 'string') {
                    callback?.({ ok: false, error: 'invalid-args' });
                    return;
                }

                const session = await db.session.findUnique({
                    where: { id: sid, accountId: userId }
                });
                if (!session) {
                    callback?.({ ok: false, error: 'session-not-found' });
                    return;
                }

                const useLocalId = typeof localId === 'string' ? localId : null;
                if (useLocalId) {
                    const existing = await db.sessionPendingMessage.findFirst({
                        where: { sessionId: sid, localId: useLocalId }
                    });
                    if (existing) {
                        callback?.({ ok: true, id: existing.id });
                        return;
                    }
                }

                const pending = await db.sessionPendingMessage.create({
                    data: {
                        sessionId: sid,
                        localId: useLocalId,
                        content: { t: 'encrypted', c: message }
                    }
                });

                const count = await db.sessionPendingMessage.count({ where: { sessionId: sid } });
                eventRouter.emitEphemeral({
                    userId,
                    payload: buildPendingQueueEphemeral(sid, count),
                    recipientFilter: { type: 'all-interested-in-session', sessionId: sid },
                });

                callback?.({ ok: true, id: pending.id });
            } catch (error) {
                log({ module: 'websocket', level: 'error' }, `Error in pending-enqueue handler: ${error}`);
                callback?.({ ok: false, error: 'error' });
            }
        });
    });

    socket.on('pending-list', async (data: any, callback: (response: any) => void) => {
        await pendingQueueLock.inLock(async () => {
            try {
                websocketEventsCounter.inc({ event_type: 'pending-list' });
                const { sid, limit } = data ?? {};

                if (!sid || typeof sid !== 'string') {
                    callback?.({ ok: false, error: 'invalid-args' });
                    return;
                }

                const session = await db.session.findUnique({
                    where: { id: sid, accountId: userId }
                });
                if (!session) {
                    callback?.({ ok: false, error: 'session-not-found' });
                    return;
                }

                const take = typeof limit === 'number' && Number.isFinite(limit) ? Math.max(1, Math.min(limit, 200)) : 50;

                const pending = await db.sessionPendingMessage.findMany({
                    where: { sessionId: sid },
                    orderBy: [{ createdAt: 'asc' }, { id: 'asc' }],
                    take,
                    select: {
                        id: true,
                        localId: true,
                        content: true,
                        createdAt: true,
                        updatedAt: true,
                    }
                });

                callback?.({
                    ok: true,
                    messages: pending.map((p) => ({
                        id: p.id,
                        localId: p.localId,
                        message: (p.content as any)?.c,
                        createdAt: p.createdAt.getTime(),
                        updatedAt: p.updatedAt.getTime(),
                    }))
                });
            } catch (error) {
                log({ module: 'websocket', level: 'error' }, `Error in pending-list handler: ${error}`);
                callback?.({ ok: false, error: 'error' });
            }
        });
    });

    socket.on('pending-update', async (data: any, callback: (response: any) => void) => {
        await pendingQueueLock.inLock(async () => {
            try {
                websocketEventsCounter.inc({ event_type: 'pending-update' });
                const { sid, id, message } = data ?? {};

                if (!sid || typeof sid !== 'string' || !id || typeof id !== 'string' || typeof message !== 'string') {
                    callback?.({ ok: false, error: 'invalid-args' });
                    return;
                }

                const session = await db.session.findUnique({
                    where: { id: sid, accountId: userId }
                });
                if (!session) {
                    callback?.({ ok: false, error: 'session-not-found' });
                    return;
                }

                await db.sessionPendingMessage.updateMany({
                    where: { id, sessionId: sid },
                    data: { content: { t: 'encrypted', c: message } }
                });

                callback?.({ ok: true });
            } catch (error) {
                log({ module: 'websocket', level: 'error' }, `Error in pending-update handler: ${error}`);
                callback?.({ ok: false, error: 'error' });
            }
        });
    });

    socket.on('pending-delete', async (data: any, callback: (response: any) => void) => {
        await pendingQueueLock.inLock(async () => {
            try {
                websocketEventsCounter.inc({ event_type: 'pending-delete' });
                const { sid, id } = data ?? {};

                if (!sid || typeof sid !== 'string' || !id || typeof id !== 'string') {
                    callback?.({ ok: false, error: 'invalid-args' });
                    return;
                }

                const session = await db.session.findUnique({
                    where: { id: sid, accountId: userId }
                });
                if (!session) {
                    callback?.({ ok: false, error: 'session-not-found' });
                    return;
                }

                await db.sessionPendingMessage.deleteMany({
                    where: { id, sessionId: sid }
                });

                const count = await db.sessionPendingMessage.count({ where: { sessionId: sid } });
                eventRouter.emitEphemeral({
                    userId,
                    payload: buildPendingQueueEphemeral(sid, count),
                    recipientFilter: { type: 'all-interested-in-session', sessionId: sid },
                });

                callback?.({ ok: true });
            } catch (error) {
                log({ module: 'websocket', level: 'error' }, `Error in pending-delete handler: ${error}`);
                callback?.({ ok: false, error: 'error' });
            }
        });
    });

    socket.on('pending-pop', async (data: any, callback: (response: any) => void) => {
        await pendingQueueLock.inLock(async () => {
            try {
                websocketEventsCounter.inc({ event_type: 'pending-pop' });
                const { sid } = data ?? {};

                if (connection.connectionType !== 'session-scoped') {
                    callback?.({ ok: false, error: 'forbidden' });
                    return;
                }

                if (!sid || typeof sid !== 'string') {
                    callback?.({ ok: false, error: 'invalid-args' });
                    return;
                }

                const session = await db.session.findUnique({
                    where: { id: sid, accountId: userId }
                });
                if (!session) {
                    callback?.({ ok: false, error: 'session-not-found' });
                    return;
                }

                const result = await db.$transaction(async (tx) => {
                    const pending = await tx.sessionPendingMessage.findFirst({
                        where: { sessionId: sid },
                        orderBy: [{ createdAt: 'asc' }, { id: 'asc' }],
                    });
                    if (!pending) {
                        return { popped: false as const };
                    }

                    // Remove from pending queue first (so repeated pop doesn't duplicate)
                    await tx.sessionPendingMessage.delete({ where: { id: pending.id } });

                    const updSeq = (await tx.account.update({
                        where: { id: userId },
                        select: { seq: true },
                        data: { seq: { increment: 1 } }
                    })).seq;

                    const msgSeq = (await tx.session.update({
                        where: { id: sid },
                        select: { seq: true },
                        data: { seq: { increment: 1 } }
                    })).seq;

                    const msg = await tx.sessionMessage.create({
                        data: {
                            sessionId: sid,
                            seq: msgSeq,
                            content: pending.content as any,
                            localId: pending.localId ?? null
                        }
                    });

                    const updatePayload = buildNewMessageUpdate(msg, sid, updSeq, randomKeyNaked(12));
                    eventRouter.emitUpdate({
                        userId,
                        payload: updatePayload,
                        recipientFilter: { type: 'all-interested-in-session', sessionId: sid },
                        // Do NOT skip sender connection: the agent needs to receive the created message.
                    });

                    return { popped: true as const };
                });

                const count = await db.sessionPendingMessage.count({ where: { sessionId: sid } });
                eventRouter.emitEphemeral({
                    userId,
                    payload: buildPendingQueueEphemeral(sid, count),
                    recipientFilter: { type: 'all-interested-in-session', sessionId: sid },
                });

                callback?.({ ok: true, popped: result.popped });
            } catch (error) {
                log({ module: 'websocket', level: 'error' }, `Error in pending-pop handler: ${error}`);
                callback?.({ ok: false, error: 'error' });
            }
        });
    });

    socket.on('session-end', async (data: {
        sid: string;
        time: number;
    }) => {
        try {
            const { sid, time } = data;
            let t = time;
            if (typeof t !== 'number') {
                return;
            }
            if (t > Date.now()) {
                t = Date.now();
            }
            if (t < Date.now() - 1000 * 60 * 10) { // Ignore if time is in the past 10 minutes
                return;
            }

            // Resolve session
            const session = await db.session.findUnique({
                where: { id: sid, accountId: userId }
            });
            if (!session) {
                return;
            }

            // Update last active at
            await db.session.update({
                where: { id: sid },
                data: { lastActiveAt: new Date(t), active: false }
            });

            // Emit session activity update
            const sessionActivity = buildSessionActivityEphemeral(sid, false, t, false);
            eventRouter.emitEphemeral({
                userId,
                payload: sessionActivity,
                recipientFilter: { type: 'user-scoped-only' }
            });
        } catch (error) {
            log({ module: 'websocket', level: 'error' }, `Error in session-end: ${error}`);
        }
    });

}
