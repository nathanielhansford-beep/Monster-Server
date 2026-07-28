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
