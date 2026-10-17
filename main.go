// Reproduction for https://github.com/nats-io/nats.go/issues/2063: a JetStream
// Consume() can stall forever after its connection status listener loses events
// (fixed by https://github.com/nats-io/nats.go/pull/2193).
//
//  1. A reconnect happens while the Consume() status goroutine is busy in
//     ConsumeErrHandler. sendStatusEvent's closed-channel check consumes the
//     unread RECONNECTING event and drops the listener for CONNECTED.
//  2. Any later reconnect: the consumer sees RECONNECTING (stops its heartbeat
//     monitor) but never CONNECTED, so it never re-pulls. If the server dropped
//     the pull request meanwhile, nothing ever arrives again.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	streamName   = "REPRO"
	consumerName = "REPRO"
	pullExpiry   = 4 * time.Second
	heartbeat    = time.Second
	slowHandler  = 8 * time.Second
	outage       = 10 * time.Second
)

var (
	start   = time.Now()
	servers = []string{"127.0.0.1:14221", "127.0.0.1:14222", "127.0.0.1:14223"}
)

func logf(format string, args ...any) {
	fmt.Printf("[%6.2fs] %s\n", time.Since(start).Seconds(), fmt.Sprintf(format, args...))
}

func main() {
	if err := run(); err != nil {
		fmt.Println("error:", err)
		os.Exit(2)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Publisher and management traffic go straight to the cluster.
	mgmt, err := nats.Connect(strings.Join(servers, ","), nats.MaxReconnects(-1))
	if err != nil {
		return err
	}
	defer mgmt.Close()
	mjs, _ := jetstream.New(mgmt)
	_ = mjs.DeleteStream(ctx, streamName)
	if _, err := mjs.CreateStream(ctx, jetstream.StreamConfig{
		Name: streamName, Subjects: []string{"repro.>"}, Replicas: 3,
	}); err != nil {
		return err
	}
	if _, err := mjs.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable: consumerName, AckPolicy: jetstream.AckExplicitPolicy, Replicas: 3,
	}); err != nil {
		return err
	}

	// The consumer connects through local proxies so we can break its network
	// without touching the cluster.
	proxies := map[string]*proxy{}
	var urls []string
	for _, s := range servers {
		p, err := newProxy(s)
		if err != nil {
			return err
		}
		proxies[p.addr] = p
		urls = append(urls, "nats://"+p.addr)
	}
	nc, err := nats.Connect(strings.Join(urls, ","),
		nats.DontRandomize(),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.PingInterval(time.Second),
		nats.MaxPingsOutstanding(2),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { logf("client: disconnected (%v)", err) }),
		nats.ReconnectHandler(func(nc *nats.Conn) { logf("client: reconnected to %s", nc.ConnectedServerName()) }),
	)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, _ := jetstream.New(nc)
	cons, err := js.Consumer(ctx, streamName, consumerName)
	if err != nil {
		return err
	}

	var delivered, lastMsg atomic.Int64
	var slept atomic.Bool
	handlerBusy := make(chan struct{}, 1)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		delivered.Add(1)
		lastMsg.Store(time.Now().UnixNano())
		_ = msg.Ack()
	},
		jetstream.PullExpiry(pullExpiry),
		jetstream.PullHeartbeat(heartbeat),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			logf("ConsumeErrHandler: %v", err)
			// Stand-in for anything that keeps the Consume() status goroutine busy
			// (slow logging, metrics, alerting...) at the wrong moment.
			if errors.Is(err, jetstream.ErrNoHeartbeat) && slept.CompareAndSwap(false, true) {
				logf("ConsumeErrHandler: busy for %v", slowHandler)
				handlerBusy <- struct{}{}
				time.Sleep(slowHandler)
				logf("ConsumeErrHandler: done")
			}
		}))
	if err != nil {
		return err
	}
	defer cc.Stop()
	statusCh := consumeStatusListener(nc)
	logf("Consume() status listener: %s", registration(nc, statusCh))

	go publish(ctx, mjs)
	go report(ctx, mjs, nc, &delivered)
	time.Sleep(3 * time.Second)

	current := func() *proxy { return proxies[strings.TrimPrefix(nc.ConnectedUrl(), "nats://")] }

	p := current()
	logf("=== STEP 1: freeze the consumer's link to %s (no disconnect, traffic just stops)", nc.ConnectedServerName())
	p.freeze(true)
	reconnects := nc.Stats().Reconnects
	for nc.Stats().Reconnects == reconnects || !nc.IsConnected() {
		time.Sleep(10 * time.Millisecond)
	}
	p.freeze(false)
	select {
	case <-handlerBusy:
	default:
		return errors.New("ConsumeErrHandler was not busy during the reconnect; timing assumptions broken")
	}
	time.Sleep(slowHandler)
	logf("Consume() status listener: %s", registration(nc, statusCh))

	logf("=== STEP 2: consumer network outage for %v (longer than pull expiry %v)", outage, pullExpiry)
	for _, p := range proxies {
		p.down(true)
	}
	time.Sleep(outage)
	for _, p := range proxies {
		p.down(false)
	}
	logf("network restored")
	time.Sleep(20 * time.Second)

	idle := time.Since(time.Unix(0, lastMsg.Load()))
	logf("Consume() status listener: %s", registration(nc, statusCh))
	if idle > 10*time.Second {
		logf("RESULT: STALLED - no message for %v while the connection is healthy", idle.Round(time.Second))
		os.Exit(1)
	}
	logf("RESULT: OK - consumer recovered (last message %v ago)", idle.Round(100*time.Millisecond))
	return nil
}

func publish(ctx context.Context, js jetstream.JetStream) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pctx, cancel := context.WithTimeout(ctx, time.Second)
			_, _ = js.Publish(pctx, "repro.msg", []byte("hello"))
			cancel()
		}
	}
}

func report(ctx context.Context, js jetstream.JetStream, nc *nats.Conn, delivered *atomic.Int64) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			line := fmt.Sprintf("delivered=%-5d connected_to=%-3s", delivered.Load(), nc.ConnectedServerName())
			ictx, cancel := context.WithTimeout(ctx, time.Second)
			if c, err := js.Consumer(ictx, streamName, consumerName); err == nil {
				if ci, err := c.Info(ictx); err == nil {
					line += fmt.Sprintf(" | server: num_pending=%-4d num_waiting=%d", ci.NumPending, ci.NumWaiting)
				}
			}
			cancel()
			logf("%s", line)
		}
	}
}

// proxy is a TCP proxy that can freeze (stop forwarding, keep sockets open) or
// go down (drop connections and refuse new ones).
type proxy struct {
	addr, target string
	frozen, dead atomic.Bool
	mu           sync.Mutex
	conns        map[net.Conn]struct{}
}

func newProxy(target string) (*proxy, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &proxy{addr: l.Addr().String(), target: target, conns: map[net.Conn]struct{}{}}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go p.serve(c)
		}
	}()
	return p, nil
}

func (p *proxy) serve(client net.Conn) {
	if p.dead.Load() {
		client.Close()
		return
	}
	server, err := net.Dial("tcp", p.target)
	if err != nil {
		client.Close()
		return
	}
	p.mu.Lock()
	p.conns[client], p.conns[server] = struct{}{}, struct{}{}
	p.mu.Unlock()
	go p.pipe(client, server)
	p.pipe(server, client)
}

func (p *proxy) pipe(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		for p.frozen.Load() {
			time.Sleep(10 * time.Millisecond)
		}
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *proxy) freeze(on bool) { p.frozen.Store(on) }

func (p *proxy) down(on bool) {
	p.dead.Store(on)
	if !on {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		c.Close()
		delete(p.conns, c)
	}
}

// statusListeners reads nc.statListeners (unexported) purely as a diagnostic.
func statusListeners(nc *nats.Conn) map[nats.Status]map[chan nats.Status]bool {
	v := reflect.ValueOf(nc).Elem()
	mu := (*sync.RWMutex)(unsafe.Pointer(v.FieldByName("mu").UnsafeAddr()))
	f := v.FieldByName("statListeners")
	sl := reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
	mu.Lock()
	defer mu.Unlock()
	out := map[nats.Status]map[chan nats.Status]bool{}
	for _, k := range sl.MapKeys() {
		s := nats.Status(k.Int())
		out[s] = map[chan nats.Status]bool{}
		for _, ch := range sl.MapIndex(k).MapKeys() {
			out[s][ch.Interface().(chan nats.Status)] = true
		}
	}
	return out
}

// consumeStatusListener returns the channel Consume() registered via nc.StatusChanged.
func consumeStatusListener(nc *nats.Conn) chan nats.Status {
	for ch := range statusListeners(nc)[nats.RECONNECTING] {
		return ch
	}
	return nil
}

func registration(nc *nats.Conn, ch chan nats.Status) string {
	ls := statusListeners(nc)
	return fmt.Sprintf("registered for RECONNECTING=%v CONNECTED=%v", ls[nats.RECONNECTING][ch], ls[nats.CONNECTED][ch])
}
