package whatsmeow_service

// registry.go — thread-safe replacements for the service-wide shared maps
// (clientPointer / myClientPointer / killChannel) that caused the production
// crash `fatal error: concurrent map read and map write`.
//
// Design rules (do not break them):
//   - The internal mutex is held ONLY for the map operation itself. It is
//     never held across websocket calls (Connect/Disconnect/SendMessage),
//     event handlers, HTTP round-trips, or time.Sleep.
//   - Kill channels are buffered (cap 1) and are NEVER closed. Senders use
//     Signal(), a non-blocking send: if a signal is already pending the kill
//     is already going to happen, so dropping is safe. This makes the classic
//     "close of closed channel" and "send on closed channel" panics
//     impossible by construction.
//   - A monitor goroutine always captures its generation's channel/client and
//     exits when the registry no longer points at its generation
//     (stale-generation check), so superseded goroutines cannot touch a newer
//     client.
//   - Per-instance lifecycle operations (teardown + recreate) are serialized
//     by InstanceLocks: at most one reconnect/start critical section runs per
//     instance, while different instances proceed fully in parallel.

import (
	"sync"

	"go.mau.fi/whatsmeow"
)

// ---------------------------------------------------------------------------
// ClientRegistry: instanceID -> *whatsmeow.Client
// ---------------------------------------------------------------------------

type ClientRegistry struct {
	mu      sync.RWMutex
	clients map[string]*whatsmeow.Client
}

func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{clients: make(map[string]*whatsmeow.Client)}
}

// Get returns the client for instanceID, or nil when absent.
func (r *ClientRegistry) Get(instanceID string) *whatsmeow.Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.clients[instanceID]
}

func (r *ClientRegistry) Set(instanceID string, client *whatsmeow.Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[instanceID] = client
}

func (r *ClientRegistry) Delete(instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, instanceID)
}

// DeleteIf removes the entry only when it still points at `client`, so a stale
// goroutine can never delete a newer generation's client.
func (r *ClientRegistry) DeleteIf(instanceID string, client *whatsmeow.Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.clients[instanceID] == client {
		delete(r.clients, instanceID)
	}
}

// ---------------------------------------------------------------------------
// MyClientRegistry: instanceID -> *MyClient
// ---------------------------------------------------------------------------

type MyClientRegistry struct {
	mu      sync.RWMutex
	clients map[string]*MyClient
}

func NewMyClientRegistry() *MyClientRegistry {
	return &MyClientRegistry{clients: make(map[string]*MyClient)}
}

func (r *MyClientRegistry) Get(instanceID string) *MyClient {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.clients[instanceID]
}

func (r *MyClientRegistry) Set(instanceID string, client *MyClient) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[instanceID] = client
}

func (r *MyClientRegistry) Delete(instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, instanceID)
}

// DeleteIf removes the entry only when it still points at `client`.
func (r *MyClientRegistry) DeleteIf(instanceID string, client *MyClient) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.clients[instanceID] == client {
		delete(r.clients, instanceID)
	}
}

// ---------------------------------------------------------------------------
// KillRegistry: instanceID -> chan bool (buffered, never closed)
// ---------------------------------------------------------------------------

type KillRegistry struct {
	mu       sync.RWMutex
	channels map[string]chan bool
}

func NewKillRegistry() *KillRegistry {
	return &KillRegistry{channels: make(map[string]chan bool)}
}

// Get returns the current generation's channel, or nil when absent.
func (r *KillRegistry) Get(instanceID string) chan bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.channels[instanceID]
}

// Replace creates a FRESH buffered channel for a new client generation and
// returns it. The caller (StartClient) is the only creator; every other
// participant either signals or observes.
func (r *KillRegistry) Replace(instanceID string) chan bool {
	ch := make(chan bool, 1)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.channels[instanceID] = ch
	return ch
}

// Signal performs a non-blocking send. Returns false when no channel exists
// or a signal is already pending (in both cases the kill is already handled
// or irrelevant).
func (r *KillRegistry) Signal(instanceID string) bool {
	r.mu.RLock()
	ch, ok := r.channels[instanceID]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	select {
	case ch <- true:
		return true
	default:
		return true // already pending — the kill is happening
	}
}

func (r *KillRegistry) Delete(instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.channels, instanceID)
}

// DeleteIf removes the entry only when it still holds `ch`.
func (r *KillRegistry) DeleteIf(instanceID string, ch chan bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.channels[instanceID] == ch {
		delete(r.channels, instanceID)
	}
}

// ---------------------------------------------------------------------------
// InstanceLocks: per-instance mutex. Serializes lifecycle operations
// (teardown + recreate) for the SAME instance without blocking other
// instances. Entries are never removed (bounded by the number of instances).
// ---------------------------------------------------------------------------

type InstanceLocks struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func NewInstanceLocks() *InstanceLocks {
	return &InstanceLocks{locks: make(map[string]*sync.Mutex)}
}

// Lock acquires the per-instance mutex and returns the unlock function.
func (l *InstanceLocks) Lock(instanceID string) func() {
	l.mu.Lock()
	m, ok := l.locks[instanceID]
	if !ok {
		m = &sync.Mutex{}
		l.locks[instanceID] = m
	}
	l.mu.Unlock()
	m.Lock()
	return m.Unlock
}

// ---------------------------------------------------------------------------
// ReconnectTracker: guarantees at most one reconnect "in flight" per
// instance. TryStart returns false when a reconnect is already running, so
// bursts of events.Disconnected collapse into a single recovery.
// ---------------------------------------------------------------------------

type ReconnectTracker struct {
	mu      sync.Mutex
	inFlight map[string]bool
}

func NewReconnectTracker() *ReconnectTracker {
	return &ReconnectTracker{inFlight: make(map[string]bool)}
}

func (t *ReconnectTracker) TryStart(instanceID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inFlight[instanceID] {
		return false
	}
	t.inFlight[instanceID] = true
	return true
}

func (t *ReconnectTracker) Finish(instanceID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.inFlight, instanceID)
}
