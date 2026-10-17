# nats.go #2063 reproduction

Minimal reproduction for [nats-io/nats.go#2063](https://github.com/nats-io/nats.go/issues/2063):
a JetStream `Consume()` stalls forever while the connection is healthy. The
fix is [nats-io/nats.go#2193](https://github.com/nats-io/nats.go/pull/2193).

- **before**: nats.go `v1.54.0`. The consumer stalls.
- **after**: nats.go at PR #2193 head (`8382c4b`). The consumer recovers.

## Run

Requires Go 1.26+ and Docker (or Podman) with compose.

```sh
make up        # 3-node JetStream cluster, nats-server 2.12.4
make before    # exits 1: RESULT: STALLED
make after     # exits 0: RESULT: OK
make down
```

For Podman, use `make up COMPOSE="podman compose"`.

## What it does

The consumer connects through small in-process TCP proxies, so the program can
break the consumer's network without touching the cluster. Publishing and
monitoring go directly to the cluster.

1. **Step 1: frozen link.** The proxy stops forwarding but keeps the sockets
   open. Heartbeats stop, so `ConsumeErrHandler` gets `ErrNoHeartbeat`. The
   handler stays busy for 8s. While it is busy, the client detects a stale
   connection and reconnects to another node.

   `Conn.sendStatusEvent` decides whether a listener channel is closed with a
   non-blocking *receive*. When it sends `CONNECTED`, it swallows the unread
   `RECONNECTING` event, treats the channel as closed, and removes it from the
   `CONNECTED` listeners. The consumer gets neither event. Nothing visible
   happens yet.
2. **Step 2: outage.** The consumer has no network for 10s, longer than the
   pull expiry. The server drops the pull request. On reconnect, the consumer
   receives `RECONNECTING`, which stops its heartbeat monitor. It never
   receives `CONNECTED`, so it never sends a new pull.

The busy handler stands in for anything that keeps the `Consume()` status
goroutine from reading its channel during a reconnect, such as slow logging,
metrics, alerting, or lock contention.

The program reads `nc.statListeners` through reflection, only as a diagnostic.
That is how it prints whether the consumer's listener is still registered.

## Expected output

before (`v1.54.0`):

```
[  0.13s] Consume() status listener: registered for RECONNECTING=true CONNECTED=true
[  3.13s] === STEP 1: freeze the consumer's link to n1 (no disconnect, traffic just stops)
[  5.08s] ConsumeErrHandler: nats: no heartbeat received
[  5.08s] ConsumeErrHandler: busy for 8s
[  5.13s] client: disconnected (nats: stale connection)
[  5.13s] client: reconnected to n2
[ 13.08s] ConsumeErrHandler: done
[ 13.14s] Consume() status listener: registered for RECONNECTING=true CONNECTED=false
[ 13.14s] === STEP 2: consumer network outage for 10s (longer than pull expiry 4s)
[ 23.14s] network restored
[ 23.65s] client: reconnected to n2
[ 24.13s] delivered=240   connected_to=n2  | server: num_pending=220  num_waiting=0
[ 32.13s] delivered=240   connected_to=n2  | server: num_pending=379  num_waiting=0
[ 42.13s] delivered=240   connected_to=n2  | server: num_pending=580  num_waiting=0
[ 43.14s] RESULT: STALLED - no message for 30s while the connection is healthy
```

after (PR #2193):

```
[ 13.15s] Consume() status listener: registered for RECONNECTING=true CONNECTED=true
[ 13.15s] === STEP 2: consumer network outage for 10s (longer than pull expiry 4s)
[ 23.15s] network restored
[ 23.71s] client: reconnected to n2
[ 24.14s] delivered=459   connected_to=n2  | server: num_pending=0    num_waiting=1
[ 42.14s] delivered=839   connected_to=n2  | server: num_pending=0    num_waiting=2
[ 43.15s] RESULT: OK - consumer recovered (last message 0s ago)
```

With the fix, consumption pauses in step 1 until the busy handler returns. The
`CONNECTED` event waits in the channel until the goroutine reads it, and then
the consumer re-pulls.

## Notes

- **The issue's original 409 diagnosis was wrong.** After a
  `409 Leadership Change`, the client already re-pulls: `checkPending()` runs
  right after `handleStatusMsg`. On this cluster, consumer leader step-downs
  resumed delivery in about 50–70 ms.
- **A crashed consumer leader does not trigger the stall.** For replicated
  consumers, the server copies pending pull requests to followers. The new
  leader sends a `409 Leadership Change` to the client, and that re-pull goes
  through the message handler, bypassing the broken listener. The stall needs a
  reconnect after which the server no longer has the pull and sends nothing, as
  in step 2.
- **The first lost event wasn't triggered without the busy handler.** About
  1,200 rapid reconnects, including with `GOMAXPROCS=1` and a CPU-heavy message
  handler, did not reproduce it. The status goroutine has to be blocked, not
  merely slow.
