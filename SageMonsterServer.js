const { WebSocketServer, WebSocket } = require("ws");

const wss = new WebSocketServer({
    port: process.env.PORT || 8080
});

const players = new Map();
const battles = new Map();

function send(ws, message) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    ws.send(JSON.stringify(message));
    return true;
}

function broadcast(message, exceptWs = null) {
    const payload = JSON.stringify(message);

    for (const client of wss.clients) {
        if (client !== exceptWs && client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    }
}

function sendToPlayer(playerId, message) {
    const entry = players.get(playerId);
    return Boolean(entry && send(entry.ws, message));
}

function makeBattleId() {
    return "b" + Math.random().toString(36).slice(2, 10);
}

function endBattleForPlayer(playerId, reason = "opponentDisconnected") {
    const entry = players.get(playerId);
    if (!entry || !entry.battleId) return;

    const battle = battles.get(entry.battleId);
    entry.battleId = null;
    if (!battle) return;

    const opponentId = battle.p1 === playerId ? battle.p2 : battle.p1;
    const opponent = players.get(opponentId);
    if (opponent) opponent.battleId = null;

    sendToPlayer(opponentId, {
        type: "battleEnd",
        battleId: battle.battleId,
        winnerId: opponentId,
        reason
    });

    battles.delete(battle.battleId);
}

wss.on("connection", (ws) => {
    ws.isAlive = true;
    ws.on("pong", () => { ws.isAlive = true; });

    ws.on("message", (raw) => {
        let data;

        try {
            data = JSON.parse(raw.toString());
        } catch (error) {
            console.error("Bad message:", error);
            return;
        }

        if (data.type === "join") {
            const playerId = String(data.playerId || "").trim();

            if (!playerId) {
                ws.close(1008, "Missing player ID");
                return;
            }

            const previous = players.get(playerId);
            if (previous && previous.ws !== ws) {
                endBattleForPlayer(playerId, "opponentReconnected");
                previous.ws.close(4001, "Replaced by a newer connection");
            }

            ws.playerId = playerId;

            const playerState = {
                playerId,
                nickname: String(data.nickname || "Trainer").slice(0, 40),
                x: Number.isFinite(Number(data.x)) ? Number(data.x) : 8,
                y: Number.isFinite(Number(data.y)) ? Number(data.y) : 18,
                dir: data.dir || "down"
            };

            players.set(playerId, { ws, state: playerState, battleId: null });

            send(ws, {
                type: "worldState",
                players: [...players.values()]
                    .filter(entry => entry.ws !== ws)
                    .map(entry => entry.state)
            });

            broadcast({ type: "playerJoined", player: playerState }, ws);
            console.log(`${playerId} joined the overworld`);
            return;
        }

        if (!ws.playerId) return;

        const self = players.get(ws.playerId);

        // Ignore messages from an older socket that has been replaced.
        if (!self || self.ws !== ws) return;

        if (data.type === "state") {
            if (Number.isFinite(Number(data.x))) self.state.x = Number(data.x);
            if (Number.isFinite(Number(data.y))) self.state.y = Number(data.y);
            self.state.dir = data.dir || self.state.dir;

            broadcast({
                type: "state",
                playerId: ws.playerId,
                x: self.state.x,
                y: self.state.y,
                dir: self.state.dir
            }, ws);
            return;
        }

        if (data.type === "battleRequest") {
            const targetId = String(data.targetId || "");
            const target = players.get(targetId);

            if (
                targetId === ws.playerId ||
                !target ||
                target.ws.readyState !== WebSocket.OPEN ||
                self.battleId ||
                target.battleId
            ) {
                send(ws, { type: "battleRequestFailed", targetId, reason: "unavailable" });
                return;
            }

            sendToPlayer(targetId, {
                type: "battleRequestReceived",
                fromId: ws.playerId,
                fromNickname: self.state.nickname
            });
            return;
        }

        if (data.type === "battleAccept") {
            const fromId = String(data.fromId || "");
            const challenger = players.get(fromId);

            if (
                fromId === ws.playerId ||
                !challenger ||
                challenger.ws.readyState !== WebSocket.OPEN ||
                self.battleId ||
                challenger.battleId
            ) {
                sendToPlayer(fromId, {
                    type: "battleDeclined",
                    byId: ws.playerId,
                    reason: "unavailable"
                });
                return;
            }

            const battleId = makeBattleId();
            const firstTurn = Math.random() < 0.5 ? fromId : ws.playerId;

            battles.set(battleId, {
                battleId,
                p1: fromId,
                p2: ws.playerId,
                turn: firstTurn,
                teams: new Map()
            });

            self.battleId = battleId;
            challenger.battleId = battleId;

            const startMsg = {
                type: "battleStart",
                battleId,
                p1: fromId,
                p2: ws.playerId,
                turn: firstTurn
            };

            sendToPlayer(fromId, startMsg);
            sendToPlayer(ws.playerId, startMsg);
            return;
        }

        if (data.type === "battleDecline") {
            const fromId = String(data.fromId || "");
            sendToPlayer(fromId, {
                type: "battleDeclined",
                byId: ws.playerId,
                reason: "declined"
            });
            return;
        }

        if (data.type === "battleTeamSync") {
            const battle = battles.get(data.battleId);
            if (!battle || self.battleId !== battle.battleId) return;
            if (battle.p1 !== ws.playerId && battle.p2 !== ws.playerId) return;
            if (!Array.isArray(data.team) || data.team.length === 0) return;

            const opponentId = battle.p1 === ws.playerId ? battle.p2 : battle.p1;
            battle.teams.set(ws.playerId, data.team);

            sendToPlayer(opponentId, {
                type: "battleTeamSync",
                battleId: battle.battleId,
                fromId: ws.playerId,
                team: data.team
            });

            // If the opponent synchronized earlier, return its stored team too.
            const opponentTeam = battle.teams.get(opponentId);
            if (opponentTeam) {
                send(ws, {
                    type: "battleTeamSync",
                    battleId: battle.battleId,
                    fromId: opponentId,
                    team: opponentTeam
                });
            }
            return;
        }

        if (data.type === "battleAction") {
            const battle = battles.get(data.battleId);
            if (!battle || self.battleId !== battle.battleId) return;
            if (battle.p1 !== ws.playerId && battle.p2 !== ws.playerId) return;
            if (!battle.teams.has(battle.p1) || !battle.teams.has(battle.p2)) return;
            if (battle.turn !== ws.playerId) return;

            const opponentId = battle.p1 === ws.playerId ? battle.p2 : battle.p1;
            battle.turn = opponentId;

            sendToPlayer(opponentId, {
                type: "battleAction",
                battleId: battle.battleId,
                fromId: ws.playerId,
                action: data.action,
                nextTurn: battle.turn
            });
            return;
        }

        if (data.type === "battleEnd") {
            const battle = battles.get(data.battleId);
            if (!battle || self.battleId !== battle.battleId) return;
            if (battle.p1 !== ws.playerId && battle.p2 !== ws.playerId) return;

            const opponentId = battle.p1 === ws.playerId ? battle.p2 : battle.p1;
            sendToPlayer(opponentId, {
                type: "battleEnd",
                battleId: battle.battleId,
                winnerId: data.winnerId,
                reason: data.reason || "finished"
            });

            const p1 = players.get(battle.p1);
            const p2 = players.get(battle.p2);
            if (p1) p1.battleId = null;
            if (p2) p2.battleId = null;

            battles.delete(battle.battleId);
        }
    });

    ws.on("close", () => {
        const playerId = ws.playerId;
        if (!playerId) return;

        const current = players.get(playerId);

        // A replaced socket must never remove the newer active connection.
        if (!current || current.ws !== ws) return;

        endBattleForPlayer(playerId);
        players.delete(playerId);
        broadcast({ type: "playerLeft", playerId });
        console.log(`${playerId} disconnected`);
    });

    ws.on("error", (error) => {
        console.error("WebSocket error:", error);
    });
});

const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
        if (!ws.isAlive) {
            ws.terminate();
            continue;
        }

        ws.isAlive = false;
        ws.ping();
    }
}, 30000);

wss.on("close", () => {
    clearInterval(heartbeat);
});

console.log("Sagemon overworld + PvP relay server running");
