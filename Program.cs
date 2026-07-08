// Relay server. Plain ASP.NET Core minimal API — no game framework, no
// external packages beyond what ships with the SDK. Runs on Kestrel, which
// works the same on Windows, Linux, and inside a Docker container on Render.
//
// What this does and does not know:
// - It knows about two kinds of tracked objects: "synced" (a client streams
//   its position continuously — players) and "predicted" (created once with
//   starting numbers and a formula computes its position forever after —
//   projectiles). It does not know what a "player" or "projectile" is
//   beyond that shape.
// - It runs one hit-detection tick on its own clock, checking whichever
//   pairs of registered objects the layer rules allow, and tells clients
//   when an overlap starts or stops. It has no opinion on what an overlap
//   means.
// - Anything else — damage, scores, wins, custom game state — rides on the
//   generic passthrough: any message type it doesn't specifically handle
//   just gets forwarded to other clients as-is. That's the "call a method
//   on everyone" tool the client API is built around.

using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://0.0.0.0:" + (Environment.GetEnvironmentVariable("PORT") ?? "8080"));
var app = builder.Build();
app.UseWebSockets();

// ─── STATE ──────────────────────────────────────────────────────────────────

var clients = new ConcurrentDictionary<string, WebSocket>();
var objects = new ConcurrentDictionary<string, TrackedObject>();
// Per-pair overlap state, so the enter/exit events only fire on a change,
// never every tick while two things stay overlapped.
var overlapState = new ConcurrentDictionary<string, bool>();
var nextPlayerNum = 0;

// ─── OBJECT MODEL ───────────────────────────────────────────────────────────
// (Declared at the end of the file — a top-level statements file requires
// every plain executable statement to come before any class/struct
// declaration, so TrackedObject has to live after app.Run(), not here.)

// ─── OVERLAP MATH ───────────────────────────────────────────────────────────
// Rect-rect is exact (standard AABB test). Anything touching an ellipse is
// an approximation, not exact geometry — true ellipse-ellipse intersection
// requires solving a system of equations, which is real complexity for
// marginal gain in a hit-detection check. Circles (equal width/height) come
// out exact under this approximation, since it reduces to plain
// distance-vs-combined-radius in that case. Worth knowing if a game leans
// hard on tightly-fitted ellipse hitboxes — this won't be pixel-perfect
// there.

static bool Overlaps(TrackedObject a, (double x, double y) posA, TrackedObject b, (double x, double y) posB)
{
    bool aIsRect = a.Shape == "rect";
    bool bIsRect = b.Shape == "rect";

    if (aIsRect && bIsRect)
    {
        return Math.Abs(posA.x - posB.x) * 2 < (a.Width + b.Width)
            && Math.Abs(posA.y - posB.y) * 2 < (a.Height + b.Height);
    }

    if (!aIsRect && !bIsRect)
    {
        // Ellipse-ellipse approximation: scale the offset by the combined
        // per-axis radii and check against a unit circle.
        double rx = (a.Width + b.Width) / 2.0;
        double ry = (a.Height + b.Height) / 2.0;
        if (rx <= 0 || ry <= 0) return false;
        double nx = (posA.x - posB.x) / rx;
        double ny = (posA.y - posB.y) / ry;
        return (nx * nx + ny * ny) < 1.0;
    }

    // One rect, one ellipse: closest-point-on-rect-to-ellipse-center approximation.
    var rect = aIsRect ? a : b;
    var rectPos = aIsRect ? posA : posB;
    var ell = aIsRect ? b : a;
    var ellPos = aIsRect ? posB : posA;

    double closestX = Math.Clamp(ellPos.x, rectPos.x - rect.Width / 2, rectPos.x + rect.Width / 2);
    double closestY = Math.Clamp(ellPos.y, rectPos.y - rect.Height / 2, rectPos.y + rect.Height / 2);
    double erx = ell.Width / 2.0;
    double ery = ell.Height / 2.0;
    if (erx <= 0 || ery <= 0) return false;
    double dnx = (closestX - ellPos.x) / erx;
    double dny = (closestY - ellPos.y) / ery;
    return (dnx * dnx + dny * dny) < 1.0;
}

// ─── MESSAGING HELPERS ──────────────────────────────────────────────────────

async Task SendTo(WebSocket socket, object message)
{
    if (socket.State != WebSocketState.Open) return;
    var json = JsonSerializer.Serialize(message);
    var bytes = Encoding.UTF8.GetBytes(json);
    try
    {
        await socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
    }
    catch
    {
        // Socket died mid-send — the receive loop for that client will
        // notice and clean it up; nothing to do here.
    }
}

async Task BroadcastExcept(string excludeId, object message)
{
    var json = JsonSerializer.Serialize(message);
    var bytes = Encoding.UTF8.GetBytes(json);
    foreach (var (id, socket) in clients)
    {
        if (id == excludeId) continue;
        if (socket.State != WebSocketState.Open) continue;
        try { await socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None); }
        catch { /* dead socket, receive loop will clean it up */ }
    }
}

async Task BroadcastAll(object message)
{
    var json = JsonSerializer.Serialize(message);
    var bytes = Encoding.UTF8.GetBytes(json);
    foreach (var (_, socket) in clients)
    {
        if (socket.State != WebSocketState.Open) continue;
        try { await socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None); }
        catch { /* dead socket, receive loop will clean it up */ }
    }
}

// ─── MESSAGE HANDLING ───────────────────────────────────────────────────────
// Everything the relay actively understands lives here. Any message type
// not listed falls through to a plain broadcast — that's the generic
// "call a method on everyone" tool the client API is built on.

async Task HandleMessage(string senderId, string json)
{
    JsonDocument doc;
    try { doc = JsonDocument.Parse(json); }
    catch { return; } // malformed message, ignore rather than crash the connection

    using (doc)
    {
        var root = doc.RootElement;
        if (!root.TryGetProperty("type", out var typeProp)) return;
        string type = typeProp.GetString() ?? "";

        switch (type)
        {
            case "sync":
            {
                string id = root.GetProperty("id").GetString() ?? senderId;
                var obj = objects.GetOrAdd(id, _ => new TrackedObject { Id = id, OwnerId = senderId, IsPredicted = false });
                obj.X = root.GetProperty("x").GetDouble();
                obj.Y = root.GetProperty("y").GetDouble();
                obj.Vx = root.TryGetProperty("vx", out var vxEl) ? vxEl.GetDouble() : 0;
                obj.Vy = root.TryGetProperty("vy", out var vyEl) ? vyEl.GetDouble() : 0;
                await BroadcastExcept(senderId, RawPassthrough(senderId, root));
                break;
            }

            case "predict":
            {
                string id = root.GetProperty("id").GetString() ?? "";
                if (id == "") return;
                var obj = new TrackedObject
                {
                    Id = id,
                    OwnerId = senderId,
                    IsPredicted = true,
                    StartX = root.GetProperty("x").GetDouble(),
                    StartY = root.GetProperty("y").GetDouble(),
                    StartVx = root.GetProperty("vx").GetDouble(),
                    StartVy = root.GetProperty("vy").GetDouble(),
                    Gravity = root.TryGetProperty("gravity", out var gEl) ? gEl.GetDouble() : 0,
                    FireTimeMs = root.TryGetProperty("firedAt", out var fEl) ? fEl.GetInt64() : DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                };
                objects[id] = obj;
                await BroadcastExcept(senderId, RawPassthrough(senderId, root));
                break;
            }

            case "hitbox":
            {
                string id = root.GetProperty("id").GetString() ?? "";
                if (!objects.TryGetValue(id, out var obj)) return; // object must exist first (sync or predict comes before hitbox registration)
                obj.Shape = root.GetProperty("shape").GetString();
                obj.Width = root.GetProperty("width").GetDouble();
                obj.Height = root.GetProperty("height").GetDouble();
                obj.Layer = root.TryGetProperty("layer", out var lEl) ? lEl.GetInt32() : 0;
                obj.TriggeredByLayers.Clear();
                if (root.TryGetProperty("triggeredBy", out var tbEl) && tbEl.ValueKind == JsonValueKind.Array)
                {
                    foreach (var v in tbEl.EnumerateArray()) obj.TriggeredByLayers.Add(v.GetInt32());
                }
                // Relay-only bookkeeping — no broadcast needed, clients don't need to know about hitbox registration.
                break;
            }

            case "delete":
            {
                string id = root.GetProperty("id").GetString() ?? "";
                objects.TryRemove(id, out _);
                await BroadcastExcept(senderId, RawPassthrough(senderId, root));
                break;
            }

            default:
            {
                // Generic passthrough — the "call a method on everyone" tool.
                // The relay doesn't need to understand this message at all.
                bool includeSelf = root.TryGetProperty("includeSelf", out var incEl) && incEl.GetBoolean();
                if (includeSelf) await BroadcastAll(RawPassthrough(senderId, root));
                else await BroadcastExcept(senderId, RawPassthrough(senderId, root));
                break;
            }
        }
    }
}

// Re-wraps an incoming message with the sender's id attached, so receivers
// know who it came from, then forwards the rest of the fields as-is.
object RawPassthrough(string senderId, JsonElement root)
{
    var dict = new Dictionary<string, object?>();
    foreach (var prop in root.EnumerateObject())
    {
        dict[prop.Name] = JsonSerializer.Deserialize<object>(prop.Value.GetRawText());
    }
    dict["from"] = senderId;
    return dict;
}

// ─── HIT DETECTION TICK ─────────────────────────────────────────────────────
// Runs on its own clock, independent of incoming messages — a predicted
// object moves purely from time passing, so a message-triggered check alone
// could miss it entirely.

const int TICK_MS = 33; // ~30Hz

async Task HitDetectionLoop()
{
    while (true)
    {
        await Task.Delay(TICK_MS);
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var withHitboxes = objects.Values.Where(o => o.Shape != null).ToList();

        for (int i = 0; i < withHitboxes.Count; i++)
        {
            for (int j = i + 1; j < withHitboxes.Count; j++)
            {
                var a = withHitboxes[i];
                var b = withHitboxes[j];
                bool relevant = a.TriggeredByLayers.Contains(b.Layer) || b.TriggeredByLayers.Contains(a.Layer);
                if (!relevant) continue;

                bool overlapping = Overlaps(a, a.PositionAt(now), b, b.PositionAt(now));
                string pairKey = string.CompareOrdinal(a.Id, b.Id) < 0 ? $"{a.Id}|{b.Id}" : $"{b.Id}|{a.Id}";
                bool wasOverlapping = overlapState.TryGetValue(pairKey, out var prev) && prev;

                if (overlapping != wasOverlapping)
                {
                    overlapState[pairKey] = overlapping;
                    await BroadcastAll(new { type = "overlap", a = a.Id, b = b.Id, state = overlapping ? "enter" : "exit" });
                }
            }
        }
    }
}

// ─── CONNECTION HANDLING ────────────────────────────────────────────────────

app.Map("/", async context =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        return;
    }

    using var socket = await context.WebSockets.AcceptWebSocketAsync();
    string playerId = "p" + Interlocked.Increment(ref nextPlayerNum);
    clients[playerId] = socket;
    Console.WriteLine($"[RELAY] {playerId} connected. Total clients: {clients.Count}");
    await SendTo(socket, new { type = "assigned", id = playerId });

    var buffer = new byte[8192];
    try
    {
        while (socket.State == WebSocketState.Open)
        {
            var result = await socket.ReceiveAsync(buffer, CancellationToken.None);
            if (result.MessageType == WebSocketMessageType.Close) break;
            string json = Encoding.UTF8.GetString(buffer, 0, result.Count);
            await HandleMessage(playerId, json);
        }
    }
    catch
    {
        // Connection dropped without a clean close handshake — treated the
        // same as a normal disconnect below.
    }
    finally
    {
        clients.TryRemove(playerId, out _);
        var owned = objects.Values.Where(o => o.OwnerId == playerId).Select(o => o.Id).ToList();
        foreach (var id in owned)
        {
            objects.TryRemove(id, out _);
            await BroadcastExcept(playerId, new { type = "delete", id, from = playerId });
        }
        Console.WriteLine($"[RELAY] {playerId} disconnected. Total clients: {clients.Count}");
    }
});

_ = Task.Run(HitDetectionLoop);

Console.WriteLine("Relay starting...");
app.Run();

// ─── OBJECT MODEL ───────────────────────────────────────────────────────────
// Has to live down here — a top-level statements file requires every plain
// executable statement to come before any class/struct declaration, and
// app.Run() above is the last statement. C# allows using a type before its
// declaration within the same file, so everything earlier in the file that
// references TrackedObject works fine regardless of where this sits.

class TrackedObject
{
    public string Id = "";
    public string OwnerId = "";
    public bool IsPredicted;

    // Synced fields — meaningful when IsPredicted is false
    public double X, Y, Vx, Vy;

    // Predicted fields — meaningful when IsPredicted is true. Position is
    // never stored directly; it's computed fresh from these every time
    // anyone asks, using elapsed time since FireTimeMs.
    public double StartX, StartY, StartVx, StartVy, Gravity;
    public long FireTimeMs;

    // Hitbox — Shape is null until RegisterHitbox has been called for this
    // object, meaning it's excluded from hit detection until opted in.
    public string? Shape; // "rect" or "ellipse"
    public double Width, Height;
    public int Layer;
    public HashSet<int> TriggeredByLayers = new HashSet<int>();

    public (double x, double y) PositionAt(long nowMs)
    {
        if (!IsPredicted) return (X, Y);
        double t = (nowMs - FireTimeMs) / 1000.0;
        double x = StartX + StartVx * t;
        double y = StartY + StartVy * t + 0.5 * Gravity * t * t;
        return (x, y);
    }
}
