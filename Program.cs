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
using System.Threading.Channels;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://0.0.0.0:" + (Environment.GetEnvironmentVariable("PORT") ?? "8080"));
var app = builder.Build();
app.UseWebSockets();

// ─── STATE ──────────────────────────────────────────────────────────────────

var clients = new ConcurrentDictionary<string, WebSocket>();
var lastSeen = new ConcurrentDictionary<string, DateTimeOffset>(); // playerId -> last time ANY message was received from them
var objects = new ConcurrentDictionary<string, TrackedObject>();
// Per-pair overlap state, so the enter/exit events only fire on a change,
// never every tick while two things stay overlapped.
var overlapState = new ConcurrentDictionary<string, bool>();
var nextPlayerNum = 0;

// Every connected player gets their own private outgoing mailbox and their
// own dedicated background sender (see RunOutgoingSender below). Code that
// decides to send something — hit detection, a deletion, an RPC broadcast,
// anything — never talks to a socket directly. It drops the message in the
// target player's mailbox and returns immediately, without waiting to see
// whether that message has actually reached the network yet. Only that one
// player's own dedicated sender ever reads from their mailbox and performs
// the real socket send, at whatever pace that one connection can handle.
//
// This is what guarantees a slow or stalled connection can only ever back
// up its OWN mailbox — it can never delay any other player's messages, and
// it can never delay the relay's own decision-making (hit detection,
// deletions, anything), since nothing that makes a decision ever waits on
// a socket. Replaces the previous per-client send lock entirely: with only
// one dedicated sender ever touching a given socket, there's no longer any
// concurrent-access hazard to guard against in the first place.
var outgoingQueues = new ConcurrentDictionary<string, Channel<byte[]>>();

// Serializes every read-decide-mutate-broadcast sequence that touches
// `objects`/`overlapState` for hit detection — HitDetectionLoop's own
// per-tick pass, and the "delete"/"unregisterHitbox" handling (plus
// CleanupPlayer's per-object cleanup on disconnect), all acquire this
// before doing any of that work. Without it, HitDetectionLoop runs as a
// fully independent background task with no inherent ordering relative to
// a delete arriving concurrently — a tick could observe an object as
// still genuinely present, decide to broadcast an overlap change for it,
// and a delete for that same object could be decided moments later on a
// different task, with no guarantee about which broadcast actually
// reaches a given client first. This lock removes that ambiguity
// entirely by ensuring only one of "a tick's decisions" or "an object's
// removal and its own flushed exit" can be in progress at a time,
// system-wide.
//
// Now that "broadcast" (see EnqueueSend below) only ever writes to an
// in-memory mailbox and never touches a socket, everything this lock
// guards is fast, bounded, CPU-only work — no player's connection speed
// can make another player wait on this lock for any meaningful amount of
// time. The only thing that can delay one player's processing here is
// another player's own quick, in-memory bookkeeping, never their network
// quality.
var worldLock = new SemaphoreSlim(1, 1);

// ─── OBJECT MODEL ───────────────────────────────────────────────────────────
// (Declared at the end of the file — a top-level statements file requires
// every plain executable statement to come before any class/struct
// declaration, so TrackedObject has to live after app.Run(), not here.)

// ─── OVERLAP MATH ───────────────────────────────────────────────────────────
// Rect-rect uses the Separating Axis Theorem (SAT), which is exact for
// convex shapes like rectangles regardless of rotation — checking each
// rect's two edge-normal axes and confirming no axis fully separates them.
// Ellipse-involving checks remain approximations (true rotated-ellipse
// intersection is a quartic equation, real complexity for marginal gain
// here) but now account for rotation by working in each shape's own local,
// unrotated frame rather than assuming world-axis alignment. Circles
// (equal width/height) are unaffected by rotation and remain exact.
// rotA/rotB are in degrees, 0 meaning axis-aligned (unrotated).

static (double x, double y) RotateVector(double x, double y, double degrees)
{
    double rad = degrees * Math.PI / 180.0;
    double cos = Math.Cos(rad), sin = Math.Sin(rad);
    return (x * cos - y * sin, x * sin + y * cos);
}

// Projects a rect's 4 corners onto an axis and returns [min, max].
static (double min, double max) ProjectRect((double x, double y) center, double width, double height, double rotDeg, (double x, double y) axis)
{
    double hw = width / 2.0, hh = height / 2.0;
    (double x, double y)[] localCorners = { (-hw, -hh), (hw, -hh), (hw, hh), (-hw, hh) };
    double min = double.MaxValue, max = double.MinValue;
    foreach (var c in localCorners)
    {
        var (rx, ry) = RotateVector(c.x, c.y, rotDeg);
        double wx = center.x + rx, wy = center.y + ry;
        double proj = wx * axis.x + wy * axis.y;
        if (proj < min) min = proj;
        if (proj > max) max = proj;
    }
    return (min, max);
}

static bool RectsOverlapSAT((double x, double y) posA, double widthA, double heightA, double rotA, (double x, double y) posB, double widthB, double heightB, double rotB)
{
    // Each rect contributes two candidate separating axes: its own local
    // X and Y edge normals, rotated into world space. Only 4 total axes
    // need checking for two rectangles (parallel axes between the two
    // rects would give the same result, so testing one rect's own two is
    // sufficient per rect).
    var axes = new (double x, double y)[]
    {
        RotateVector(1, 0, rotA), RotateVector(0, 1, rotA),
        RotateVector(1, 0, rotB), RotateVector(0, 1, rotB),
    };
    foreach (var axis in axes)
    {
        var (minA, maxA) = ProjectRect(posA, widthA, heightA, rotA, axis);
        var (minB, maxB) = ProjectRect(posB, widthB, heightB, rotB, axis);
        if (maxA < minB || maxB < minA) return false; // this axis separates them — no overlap possible
    }
    return true; // no separating axis found on any of the 4 candidates — they overlap
}

static bool Overlaps(TrackedObject a, (double x, double y) posA, double? rotA, TrackedObject b, (double x, double y) posB, double? rotB)
{
    bool aIsRect = a.Shape == "rectangle";
    bool bIsRect = b.Shape == "rectangle";
    double rA = rotA ?? 0.0;
    double rB = rotB ?? 0.0;

    if (aIsRect && bIsRect)
    {
        return RectsOverlapSAT(posA, a.Width, a.Height, rA, posB, b.Width, b.Height, rB);
    }

    if (!aIsRect && !bIsRect)
    {
        // Ellipse-ellipse approximation: work in A's local (unrotated)
        // frame — rotate the offset to B by -rA so A's axes align with
        // world X/Y, then also rotate B's own axes by (rB - rA) relative
        // to that frame. Reduces to the original unrotated formula when
        // both rotations are 0.
        double rx = (a.Width + b.Width) / 2.0;
        double ry = (a.Height + b.Height) / 2.0;
        if (rx <= 0 || ry <= 0) return false;
        var (offX, offY) = RotateVector(posB.x - posA.x, posB.y - posA.y, -rA);
        double nx = offX / rx;
        double ny = offY / ry;
        return (nx * nx + ny * ny) < 1.0;
    }

    // One rect, one ellipse: rotate the ellipse's center into the rect's
    // own local (unrotated) frame, run the same closest-point approximation
    // as before in that frame, then the result is rotation-invariant since
    // distance is preserved by rotation.
    var rect = aIsRect ? a : b;
    var rectPos = aIsRect ? posA : posB;
    double rectRot = aIsRect ? rA : rB;
    var ell = aIsRect ? b : a;
    var ellPos = aIsRect ? posB : posA;

    var (localEllX, localEllY) = RotateVector(ellPos.x - rectPos.x, ellPos.y - rectPos.y, -rectRot);
    double closestX = Math.Clamp(localEllX, -rect.Width / 2, rect.Width / 2);
    double closestY = Math.Clamp(localEllY, -rect.Height / 2, rect.Height / 2);
    double erx = ell.Width / 2.0;
    double ery = ell.Height / 2.0;
    if (erx <= 0 || ery <= 0) return false;
    double dnx = (closestX - localEllX) / erx;
    double dny = (closestY - localEllY) / ery;
    return (dnx * dnx + dny * dny) < 1.0;
}

// ─── SWEPT OVERLAP (PREDICTED OBJECTS ONLY) ─────────────────────────────────
// A discrete, single-instant check can tunnel: a fast projectile can be on
// one side of a target at tick N and past it at tick N+1, with no tick ever
// landing inside the hitbox. Only applied when at least one side is a
// predicted object, since a predicted object's position is a pure formula —
// PositionAt can be evaluated at any real intermediate timestamp, giving a
// true sample of the object's actual path rather than a guess. This is
// deliberately NOT applied to synced-vs-synced pairs (two players): a synced
// object's position is whatever the owning client last reported, so a large
// tick-over-tick jump could be a genuine lag spike rather than real motion —
// sweeping that segment risks registering a hit along a path the object
// never actually traveled. Discrete per-tick checking has no such risk,
// since it only ever asks "is it overlapping right now."
const int SWEEP_SAMPLES = 8; // number of sub-steps checked between lastTickMs and now, in addition to the endpoints

static bool OverlapsSwept(TrackedObject a, TrackedObject b, long lastTickMs, long now)
{
    // Explicit, not incidental: only a predicted object's position/rotation
    // is resampled across the sweep — its formula gives a genuine
    // intermediate value at each sampleMs. A synced object's position and
    // rotation are fixed at whatever it last reported, so each is
    // evaluated once at `now` and held constant for every sample, rather
    // than calling PositionAt/RotationAt(sampleMs) on it and relying on
    // that returning the same thing every time. If those ever change to
    // interpolate for synced objects, this still won't accidentally start
    // sweeping a synced object's segment.
    var fixedPosA = a.IsPredicted ? default : a.PositionAt(now);
    var fixedPosB = b.IsPredicted ? default : b.PositionAt(now);
    var fixedRotA = a.IsPredicted ? null : a.RotationAt(now);
    var fixedRotB = b.IsPredicted ? null : b.RotationAt(now);

    for (int s = 0; s <= SWEEP_SAMPLES; s++)
    {
        long sampleMs = lastTickMs + (now - lastTickMs) * s / SWEEP_SAMPLES;
        var posA = a.IsPredicted ? a.PositionAt(sampleMs) : fixedPosA;
        var posB = b.IsPredicted ? b.PositionAt(sampleMs) : fixedPosB;
        var rotA = a.IsPredicted ? a.RotationAt(sampleMs) : fixedRotA;
        var rotB = b.IsPredicted ? b.RotationAt(sampleMs) : fixedRotB;
        if (Overlaps(a, posA, rotA, b, posB, rotB)) return true;
    }
    return false;
}

// ─── MESSAGING HELPERS ──────────────────────────────────────────────────────

// The only thing any decision-making code ever does to send a message:
// drop the bytes in that one client's mailbox and return. Synchronous and
// effectively instant (an in-memory queue write) — never touches the
// network, never waits on anything. If the client's mailbox no longer
// exists (already disconnected/cleaned up), this silently does nothing;
// there's nobody left to send to.
void EnqueueSend(string clientId, byte[] bytes)
{
    if (outgoingQueues.TryGetValue(clientId, out var channel))
    {
        channel.Writer.TryWrite(bytes);
    }
}

// The one and only place that actually calls SendAsync for a given
// client — started once per connection (see the connection handler
// below) and runs for that client's entire session, reading its own
// mailbox and nothing else. Because exactly one task ever sends on a
// given socket, there's no concurrent-access hazard to guard against —
// no lock needed. A slow SendAsync here only ever delays the NEXT message
// in THIS SAME client's own mailbox; it cannot affect any other client's
// sender, since each one is a fully independent task with its own queue.
async Task RunOutgoingSender(WebSocket socket, ChannelReader<byte[]> reader)
{
    try
    {
        await foreach (var bytes in reader.ReadAllAsync())
        {
            if (socket.State != WebSocketState.Open) continue; // connection's on its way out — drop and keep draining, cleanup will catch it
            try
            {
                // Bounded, unlike the rest of this file's sends used to
                // be — a connection stalled badly enough to hit this
                // timeout is as good as dead anyway, and this is what
                // keeps this task guaranteed to actually finish in
                // bounded time when a connection closes (see the
                // connection handler below, which waits for this task to
                // end before disposing the socket).
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await socket.SendAsync(bytes, WebSocketMessageType.Text, true, cts.Token);
            }
            catch
            {
                // Socket died mid-send, or the send timed out — either
                // way the receive loop for this client will notice and
                // clean it up; nothing to do here.
            }
        }
    }
    catch
    {
        // Mailbox faulted — nothing further to send for this client.
    }
}

// Signatures are unchanged (still Task-returning, still awaitable at every
// call site) even though nothing inside these actually waits on real
// network I/O anymore — every call site below can keep its existing
// `await`, and that await now completes essentially instantly.

Task SendTo(string clientId, object message)
{
    var json = JsonSerializer.Serialize(message);
    EnqueueSend(clientId, Encoding.UTF8.GetBytes(json));
    return Task.CompletedTask;
}

Task BroadcastExcept(string excludeId, object message)
{
    var json = JsonSerializer.Serialize(message);
    var bytes = Encoding.UTF8.GetBytes(json);
    foreach (var id in clients.Keys)
    {
        if (id == excludeId) continue;
        EnqueueSend(id, bytes);
    }
    return Task.CompletedTask;
}

Task BroadcastAll(object message)
{
    var json = JsonSerializer.Serialize(message);
    var bytes = Encoding.UTF8.GetBytes(json);
    foreach (var id in clients.Keys)
    {
        EnqueueSend(id, bytes);
    }
    return Task.CompletedTask;
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
                if (root.TryGetProperty("rot", out var rotEl)) obj.Rotation = rotEl.GetDouble();
                await BroadcastExcept(senderId, RawPassthrough(senderId, root));
                break;
            }

            case "spawnSynced":
            {
                string id = root.GetProperty("id").GetString() ?? "";
                if (id == "") return;
                var obj = new TrackedObject
                {
                    Id = id,
                    OwnerId = senderId,
                    IsPredicted = false,
                    TypeName = root.TryGetProperty("typeName", out var tnEl) ? (tnEl.GetString() ?? "") : "",
                    X = root.GetProperty("x").GetDouble(),
                    Y = root.GetProperty("y").GetDouble(),
                };
                if (root.TryGetProperty("rot", out var rotEl2)) obj.Rotation = rotEl2.GetDouble();
                objects[id] = obj;
                await BroadcastExcept(senderId, RawPassthrough(senderId, root));
                break;
            }

            case "spawnPredicted":
            {
                string id = root.GetProperty("id").GetString() ?? "";
                if (id == "") return;
                var obj = new TrackedObject
                {
                    Id = id,
                    OwnerId = senderId,
                    IsPredicted = true,
                    TypeName = root.TryGetProperty("typeName", out var tnEl2) ? (tnEl2.GetString() ?? "") : "",
                    StartX = root.GetProperty("x").GetDouble(),
                    StartY = root.GetProperty("y").GetDouble(),
                    StartVx = root.GetProperty("vx").GetDouble(),
                    StartVy = root.GetProperty("vy").GetDouble(),
                    Gravity = root.TryGetProperty("gravity", out var gEl) ? gEl.GetDouble() : 0,
                    FireTimeMs = root.TryGetProperty("firedAt", out var fEl) ? fEl.GetInt64() : DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    // The relay's OWN clock reading at the moment it
                    // receives this message — used as the t=0 reference
                    // for the relay's own internal PositionAt/RotationAt
                    // math (hit detection, late-join catch-up), entirely
                    // separate from FireTimeMs. FireTimeMs is the
                    // SHOOTER's own clock reading; comparing it directly
                    // against the relay's clock (a different physical
                    // machine, with no guaranteed agreement) silently
                    // corrupted every predicted object's internal
                    // position by however much those two clocks actually
                    // disagreed — potentially seconds, not milliseconds.
                    // RelayReceivedAtMs never gets compared against
                    // anything but the relay's own later clock readings,
                    // so there's no cross-machine comparison left in this
                    // math at all. FireTimeMs itself is untouched and
                    // still relayed verbatim to other already-connected
                    // clients, who need the true original shooter
                    // timestamp for their own peer-to-peer clock-offset
                    // correction — a separate, already-correct system.
                    RelayReceivedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    RotateWithVelocity = root.TryGetProperty("rotateWithVelocity", out var rwvEl) && rwvEl.GetBoolean(),
                };
                objects[id] = obj;
                await BroadcastExcept(senderId, RawPassthrough(senderId, root));
                break;
            }

            case "hitbox":
            {
                string id = root.GetProperty("id").GetString() ?? "";
                if (!objects.TryGetValue(id, out var obj))
                {
                    Console.WriteLine($"[RELAY][HITBOX-REG-FAIL] id={id} not found in objects dictionary — registration DROPPED. Current objects: [{string.Join(",", objects.Keys)}]");
                    return;
                }
                obj.Shape = root.GetProperty("shape").GetString();
                obj.Width = root.GetProperty("width").GetDouble();
                obj.Height = root.GetProperty("height").GetDouble();
                obj.Layer = root.TryGetProperty("layer", out var lEl) ? (lEl.GetString() ?? "") : "";
                obj.TriggeredByLayers.Clear();
                if (root.TryGetProperty("triggeredBy", out var tbEl) && tbEl.ValueKind == JsonValueKind.Array)
                {
                    foreach (var v in tbEl.EnumerateArray()) obj.TriggeredByLayers.Add(v.GetString() ?? "");
                }
                Console.WriteLine($"[RELAY][HITBOX-REG-OK] id={id} shape={obj.Shape} w={obj.Width} h={obj.Height} layer={obj.Layer} triggeredBy=[{string.Join(",", obj.TriggeredByLayers)}]");
                // Relay-only bookkeeping — no broadcast needed, clients don't need to know about hitbox registration.
                break;
            }

            case "unregisterHitbox":
            {
                // Sets Shape back to null, which is exactly what
                // HitDetectionLoop's `o.Shape != null` filter checks — so
                // this object stops taking part in hit detection without
                // removing the TrackedObject itself (it may still be a
                // perfectly live synced/predicted object, just opting out
                // of hit detection specifically). Flushed first so anyone
                // still overlapping this hitbox gets a real exit rather
                // than the pair state just silently disappearing.
                // worldLock ensures this whole sequence can't interleave
                // with a concurrently-running HitDetectionLoop tick.
                string id = root.GetProperty("id").GetString() ?? "";
                await worldLock.WaitAsync();
                try
                {
                    await FlushOverlapsForId(id);
                    if (objects.TryGetValue(id, out var obj))
                    {
                        obj.Shape = null;
                    }
                }
                finally
                {
                    worldLock.Release();
                }
                // Relay-only bookkeeping otherwise — no broadcast needed,
                // clients don't need to know about hitbox unregistration
                // itself (they'll simply stop receiving overlap events for it).
                break;
            }

            case "delete":
            {
                // Flushed first, same reasoning as unregisterHitbox — any
                // pair still overlapping this object gets a real exit
                // before the object itself disappears, rather than the
                // state just vanishing with no notification. worldLock
                // ensures this whole sequence can't interleave with a
                // concurrently-running HitDetectionLoop tick.
                //
                // Broadcasts to EVERYONE, including the sender — the
                // sender does not delete/destroy anything locally when it
                // calls DeleteSpawnedObject; it only sends this request
                // and waits for this exact broadcast to come back before
                // actually removing anything, the same as every other
                // client. That's what keeps every client's view of when
                // an object disappears in sync with each other, deleter
                // included, rather than the deleter seeing it vanish
                // instantly while everyone else catches up moments later.
                string id = root.GetProperty("id").GetString() ?? "";
                await worldLock.WaitAsync();
                try
                {
                    await FlushOverlapsForId(id);
                    objects.TryRemove(id, out _);
                }
                finally
                {
                    worldLock.Release();
                }
                await BroadcastAll(RawPassthrough(senderId, root));
                break;
            }

            case "pong":
            {
                // No-op — lastSeen was already updated generically before
                // this switch ran, which is the only thing a pong is for.
                // Handled explicitly here so it doesn't fall through to
                // the default case and get broadcast to everyone else,
                // who have no use for it.
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

// Call before an id stops taking part in hit detection (deleted,
// unregistered, or its owner disconnecting) — broadcasts a real "exit" for
// any pair currently overlapping this id, then clears that pair's state.
// Without this, a mid-overlap removal was previously silent: the pair's
// state got wiped with no notification at all, so the surviving object's
// own script never heard the overlap ended, not even as a null.
async Task FlushOverlapsForId(string id)
{
    foreach (var key in overlapState.Keys.Where(k => k.StartsWith(id + "|") || k.EndsWith("|" + id)).ToList())
    {
        bool wasOverlapping = overlapState.TryGetValue(key, out var v) && v;
        overlapState.TryRemove(key, out _);
        if (!wasOverlapping) continue; // never actually entered — nothing to exit
        var parts = key.Split('|');
        await BroadcastAll(new { type = "overlap", a = parts[0], b = parts[1], state = "exit" });
    }
}

// ─── HIT DETECTION TICK ─────────────────────────────────────────────────────
// Runs on its own clock, independent of incoming messages — a predicted
// object moves purely from time passing, so a message-triggered check alone
// could miss it entirely.

const int TICK_MS = 33; // ~30Hz

async Task HitDetectionLoop()
{
    long? lastTickMs = null; // null on the very first tick — nothing to sweep from yet, so that tick falls back to a discrete check
    long lastTickSnapshotLogMs = 0; // throttles the [TICK] snapshot log to once a second
    while (true)
    {
        await Task.Delay(TICK_MS);
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        await worldLock.WaitAsync();
        try
        {
            var withHitboxes = objects.Values.Where(o => o.Shape != null).ToList();
            // Throttled to once a second — the full snapshot every tick
            // (30/sec) was enough log volume on its own to make a hosting
            // dashboard look like it wasn't updating in real time.
            if (now - lastTickSnapshotLogMs > 1000)
            {
                lastTickSnapshotLogMs = now;
                Console.WriteLine($"[RELAY][TICK] now={now} withHitboxesCount={withHitboxes.Count} ids=[{string.Join(",", withHitboxes.Select(o => $"{o.Id}:{o.Layer}"))}]");
            }

            for (int i = 0; i < withHitboxes.Count; i++)
            {
                for (int j = i + 1; j < withHitboxes.Count; j++)
                {
                    var a = withHitboxes[i];
                    var b = withHitboxes[j];
                    bool relevant = a.TriggeredByLayers.Contains(b.Layer) || b.TriggeredByLayers.Contains(a.Layer);
                    if (!relevant) continue;

                    // Swept only when at least one side is a predicted object
                    // (see the comment on OverlapsSwept for why synced objects
                    // are deliberately excluded) and only once a previous tick
                    // exists to sweep from.
                    bool useSwept = lastTickMs.HasValue && (a.IsPredicted || b.IsPredicted);
                    bool overlapping = useSwept
                        ? OverlapsSwept(a, b, lastTickMs!.Value, now)
                        : Overlaps(a, a.PositionAt(now), a.RotationAt(now), b, b.PositionAt(now), b.RotationAt(now));

                    string pairKey = string.CompareOrdinal(a.Id, b.Id) < 0 ? $"{a.Id}|{b.Id}" : $"{b.Id}|{a.Id}";
                    bool wasOverlapping = overlapState.TryGetValue(pairKey, out var prev) && prev;

                    var posA = a.PositionAt(now);
                    var posB = b.PositionAt(now);
                    // TEMPORARILY unfiltered again — logs every relevant
                    // pair every tick, not just "close" ones, since the
                    // proximity filter may itself be hiding the real
                    // signal rather than just cutting noise.
                    double dist = Math.Sqrt(Math.Pow(posA.x - posB.x, 2) + Math.Pow(posA.y - posB.y, 2));
                    Console.WriteLine($"[RELAY][PAIR] {a.Id}(rect={a.Shape=="rectangle"},w={a.Width:F2},h={a.Height:F2}) vs {b.Id}(rect={b.Shape=="rectangle"},w={b.Width:F2},h={b.Height:F2}) — useSwept={useSwept} posA=({posA.x:F2},{posA.y:F2}) posB=({posB.x:F2},{posB.y:F2}) dist={dist:F2} overlapping={overlapping} wasOverlapping={wasOverlapping}");

                    if (overlapping != wasOverlapping)
                    {
                        // worldLock means no concurrent delete/unregister can
                        // be running while this executes, so a/b are
                        // guaranteed genuinely current for the whole of this
                        // block — no other task can remove them out from
                        // under this decision between here and the broadcast
                        // below.
                        overlapState[pairKey] = overlapping;
                        Console.WriteLine($"[RELAY][OVERLAP-SEND] {a.Id} vs {b.Id} state={(overlapping ? "enter" : "exit")}");
                        await BroadcastAll(new { type = "overlap", a = a.Id, b = b.Id, state = overlapping ? "enter" : "exit" });
                    }
                }
            }
        }
        catch (Exception ex)
        {
            // Previously nothing caught exceptions here at all — an
            // unhandled exception on any tick would silently kill this
            // ENTIRE background loop permanently, with zero error output
            // anywhere, and hit detection would just stop forever from
            // that point on. Logging it now instead of letting that
            // happen silently. The loop itself continues on the next
            // iteration rather than dying, unlike before.
            Console.WriteLine($"[RELAY][TICK-EXCEPTION] {ex.GetType().Name}: {ex.Message}\n{ex.StackTrace}");
        }
        finally
        {
            worldLock.Release();
        }
        lastTickMs = now;
    }
}

// Removes a player and everything they own, and tells everyone else.
// Idempotent — safe to call from two different places for the same player
// (the normal disconnect path and the heartbeat timeout path both call
// this), since clients.TryRemove only succeeds once; a second call for an
// already-removed player is a no-op rather than a duplicate broadcast.
async Task CleanupPlayer(string playerId)
{
    if (!clients.TryRemove(playerId, out _)) return;
    lastSeen.TryRemove(playerId, out _);
    // Completing the channel writer lets RunOutgoingSender's read loop for
    // this player end on its own once anything already queued has drained
    // (or immediately, if empty) — no need to forcibly cancel it.
    if (outgoingQueues.TryRemove(playerId, out var channel)) channel.Writer.Complete();
    var owned = objects.Values.Where(o => o.OwnerId == playerId).Select(o => o.Id).ToList();
    foreach (var id in owned)
    {
        // Same flush-before-remove as the explicit "delete" case, under
        // the same worldLock — a player disconnecting mid-overlap
        // shouldn't leave the other side of that overlap without a real
        // exit notification either, and this can't be allowed to
        // interleave with a concurrently-running HitDetectionLoop tick.
        await worldLock.WaitAsync();
        try
        {
            await FlushOverlapsForId(id);
            objects.TryRemove(id, out _);
        }
        finally
        {
            worldLock.Release();
        }
        await BroadcastExcept(playerId, new { type = "delete", id, from = playerId });
    }
    // No separate "player left" notification needed — a player's own
    // avatar is just another object they own, already covered by the loop
    // above. Whoever's listening for that object's delete message already
    // knows a player is gone, without a second, player-specific event.
    Console.WriteLine($"[RELAY] {playerId} disconnected. Total clients: {clients.Count}");
}

// ─── HEARTBEAT ──────────────────────────────────────────────────────────────
// Detects a connection that died without any warning — a killed process, a
// crash, a phone that got locked and had its network suspended, a cable
// pulled — none of which give the client any chance to send a graceful
// close. The relay pings everyone periodically; if a client goes quiet for
// too long, the relay assumes they're gone and force-closes the connection
// itself, running the exact same cleanup a normal disconnect would.

const int HEARTBEAT_INTERVAL_MS = 10000; // ping everyone every 10s
const int HEARTBEAT_TIMEOUT_MS = 25000;  // presumed dead after 25s of silence (2.5 missed beats — tolerates one slow/lost ping without a false positive)

async Task HeartbeatLoop()
{
    while (true)
    {
        await Task.Delay(HEARTBEAT_INTERVAL_MS);
        try
        {
            await BroadcastAll(new { type = "ping" });

            var now = DateTimeOffset.UtcNow;
            foreach (var id in clients.Keys)
            {
                // No lastSeen entry yet means they only just connected —
                // not stale, nothing to do.
                if (lastSeen.TryGetValue(id, out var seen) && (now - seen).TotalMilliseconds > HEARTBEAT_TIMEOUT_MS)
                {
                    Console.WriteLine($"[RELAY] {id} timed out (no message in {HEARTBEAT_TIMEOUT_MS}ms) — forcing disconnect");

                    // Cleanup and notification happen here directly, immediately
                    // — not left to the connection handler's own catch/finally
                    // noticing eventually. That matters because Abort() below
                    // isn't guaranteed to unstick a pending read right away: if
                    // the underlying TCP connection is "half-open" (this
                    // player's side is gone but the OS hasn't noticed yet — a
                    // severed network path rather than a cleanly closed one),
                    // a documented .NET issue means the stuck read can persist
                    // for minutes even after Abort() runs. Calling CleanupPlayer
                    // directly means the 25-second timeout is a real bound
                    // regardless of that. CleanupPlayer is safe to call here
                    // even though the connection handler's own finally block
                    // will *also* eventually call it once its read does
                    // unstick — the second call is a no-op, not a duplicate.
                    if (clients.TryGetValue(id, out var deadSocket))
                    {
                        deadSocket.Abort(); // best-effort attempt to actually free the OS-level resources; not depended on for correctness
                    }
                    await CleanupPlayer(id);
                }
            }
        }
        catch (Exception ex)
        {
            // Same reasoning as HitDetectionLoop's catch — without this,
            // any unhandled exception here would silently kill the entire
            // heartbeat system permanently (no more timeouts detected,
            // ever), with zero error output anywhere.
            Console.WriteLine($"[RELAY][HEARTBEAT-EXCEPTION] {ex.GetType().Name}: {ex.Message}\n{ex.StackTrace}");
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
    int playerNum = Interlocked.Increment(ref nextPlayerNum);
    string playerId = playerNum.ToString();
    clients[playerId] = socket;
    lastSeen[playerId] = DateTimeOffset.UtcNow;

    // This player's own private mailbox and dedicated sender — see the
    // comments on outgoingQueues/RunOutgoingSender above. Created before
    // anything is sent so there's always somewhere for early messages
    // (the "assigned" reply, catch-up spawns) to land.
    var outgoingChannel = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
    outgoingQueues[playerId] = outgoingChannel;
    var senderTask = Task.Run(() => RunOutgoingSender(socket, outgoingChannel.Reader));

    Console.WriteLine($"[RELAY] {playerId} connected. Total clients: {clients.Count}");
    await SendTo(playerId, new { type = "assigned", id = playerNum }); // sent as a real number, not a quoted string — this is what makes LocalPlayerId a genuine int client-side

    // Catch the newcomer up on everything that already exists by replaying
    // each object's spawn message directly — same shape as a live spawn,
    // so the client needs no separate catch-up parsing, just the same
    // spawnSynced/spawnPredicted handling it already needs anyway. This
    // covers players too, once a player's own avatar is just a spawned
    // object like anything else — no separate player-roster concept
    // needed.
    foreach (var obj in objects.Values)
    {
        if (obj.IsPredicted)
        {
            // Send the object's TRUE current state, not its original
            // launch data — a projectile that's been flying for seconds
            // needs to appear wherever it actually is right now, not
            // replay its entire flight from the original spawn point for
            // a client that's only just joining. Re-parameterizing a
            // constant-gravity trajectory at any intermediate point using
            // its exact position and true instantaneous velocity at that
            // point produces a future path mathematically identical to
            // the original — this isn't an approximation, verified by
            // hand: x(T) = x0 + vx*T and y(T) = y0 + vy0*T + 0.5*g*T²
            // both reduce algebraically to the same formula whether
            // evaluated from T=0 or restarted at any T0 with the
            // instantaneous position/velocity at T0 as the new origin.
            // firedAt = now means the only catch-up this client owes is
            // the tiny transit time of this one replay message, not the
            // object's entire real flight duration — which is what
            // MAX_PASSED_TIME_SEC was actually designed to bound.
            long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var currentPos = obj.PositionAt(nowMs);
            double elapsedSec = (nowMs - obj.RelayReceivedAtMs) / 1000.0;
            double currentVx = obj.StartVx;
            double currentVy = obj.StartVy + obj.Gravity * elapsedSec;

            await SendTo(playerId, new
            {
                type = "spawnPredicted", id = obj.Id, typeName = obj.TypeName,
                x = currentPos.x, y = currentPos.y, vx = currentVx, vy = currentVy,
                gravity = obj.Gravity, firedAt = nowMs, from = obj.OwnerId,
                rotateWithVelocity = obj.RotateWithVelocity
            });
        }
        else
        {
            if (obj.Rotation.HasValue)
            {
                await SendTo(playerId, new
                {
                    type = "spawnSynced", id = obj.Id, typeName = obj.TypeName,
                    rot = obj.Rotation.Value, x = obj.X, y = obj.Y, from = obj.OwnerId
                });
            }
            else
            {
                await SendTo(playerId, new
                {
                    type = "spawnSynced", id = obj.Id, typeName = obj.TypeName,
                    x = obj.X, y = obj.Y, from = obj.OwnerId
                });
            }
        }
    }

    var buffer = new byte[8192];
    try
    {
        while (socket.State == WebSocketState.Open)
        {
            var result = await socket.ReceiveAsync(buffer, CancellationToken.None);
            if (result.MessageType == WebSocketMessageType.Close) break;
            string json = Encoding.UTF8.GetString(buffer, 0, result.Count);
            lastSeen[playerId] = DateTimeOffset.UtcNow;
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
        await CleanupPlayer(playerId); // completes this client's outgoing channel
        await senderTask; // wait for RunOutgoingSender to actually finish (bounded by its own 10s send timeout) before this method returns and disposes the socket
    }
});

_ = Task.Run(HitDetectionLoop);
_ = Task.Run(HeartbeatLoop);

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
    public string TypeName = ""; // which prefab a late joiner should instantiate for this object

    // Synced fields — meaningful when IsPredicted is false
    public double X, Y, Vx, Vy;
    public double? Rotation; // null if this object doesn't sync rotation at all

    // Predicted fields — meaningful when IsPredicted is true. Position is
    // never stored directly; it's computed fresh from these every time
    // anyone asks, using elapsed time since RelayReceivedAtMs — see the
    // comment where this is set, in the spawnPredicted handler, for why
    // that's a different value from FireTimeMs and why that distinction
    // matters.
    public double StartX, StartY, StartVx, StartVy, Gravity;
    public long FireTimeMs;
    public long RelayReceivedAtMs;
    public bool RotateWithVelocity; // predicted objects only — see RotationAt

    // Hitbox — Shape is null until RegisterHitbox has been called for this
    // object, meaning it's excluded from hit detection until opted in.
    public string? Shape; // "rectangle" or "ellipse"
    public double Width, Height;
    public string Layer = "";
    public HashSet<string> TriggeredByLayers = new HashSet<string>();

    public (double x, double y) PositionAt(long nowMs)
    {
        if (!IsPredicted) return (X, Y);
        double t = (nowMs - RelayReceivedAtMs) / 1000.0;
        double x = StartX + StartVx * t;
        double y = StartY + StartVy * t + 0.5 * Gravity * t * t;
        return (x, y);
    }

    // Current facing in degrees, or null if this object has no rotation
    // to speak of (a synced object with Rotation unset, or a predicted
    // object not using RotateWithVelocity, or momentarily zero velocity).
    // For predicted objects, mirrors the client's instantaneous-velocity
    // formula exactly — vy changes over time under gravity, so this
    // curves along the real trajectory rather than freezing at launch
    // angle, matching what every client independently computes and draws.
    public double? RotationAt(long nowMs)
    {
        if (!IsPredicted) return Rotation;
        if (!RotateWithVelocity) return null;
        double t = (nowMs - RelayReceivedAtMs) / 1000.0;
        double instantVx = StartVx;
        double instantVy = StartVy + Gravity * t;
        if (instantVx == 0.0 && instantVy == 0.0) return null;
        return Math.Atan2(instantVy, instantVx) * (180.0 / Math.PI);
    }
}
