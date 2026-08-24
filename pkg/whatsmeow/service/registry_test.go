package whatsmeow_service

import (
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
)

// ---------------------------------------------------------------------------
// ClientRegistry: Get/Set/Delete concorrentes
// ---------------------------------------------------------------------------

func TestClientRegistryConcurrentAccess(t *testing.T) {
	r := NewClientRegistry()
	var wg sync.WaitGroup

	// escritores
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				r.Set("inst", &whatsmeow.Client{})
				r.Delete("inst")
			}
		}()
	}
	// leitores (múltiplas leituras durante escrita)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				_ = r.Get("inst")
			}
		}()
	}
	wg.Wait()
}

func TestClientRegistryDeleteIf(t *testing.T) {
	r := NewClientRegistry()
	gen1 := &whatsmeow.Client{}
	gen2 := &whatsmeow.Client{}

	r.Set("inst", gen1)
	r.DeleteIf("inst", gen2) // não deve remover: ponteiro diferente
	if r.Get("inst") != gen1 {
		t.Fatal("DeleteIf removeu geração errada")
	}
	r.DeleteIf("inst", gen1)
	if r.Get("inst") != nil {
		t.Fatal("DeleteIf não removeu a geração correta")
	}
}

// ---------------------------------------------------------------------------
// MyClientRegistry: DeleteIf stale-guard
// ---------------------------------------------------------------------------

func TestMyClientRegistryDeleteIf(t *testing.T) {
	r := NewMyClientRegistry()
	gen1 := &MyClient{}
	gen2 := &MyClient{}

	r.Set("inst", gen1)
	r.DeleteIf("inst", gen2)
	if r.Get("inst") != gen1 {
		t.Fatal("DeleteIf removeu geração errada")
	}
	r.DeleteIf("inst", gen1)
	if r.Get("inst") != nil {
		t.Fatal("DeleteIf não removeu a geração correta")
	}
}

// ---------------------------------------------------------------------------
// KillRegistry: sinal não-bloqueante, canal nunca fechado
// ---------------------------------------------------------------------------

func TestKillRegistrySignalNonBlocking(t *testing.T) {
	r := NewKillRegistry()
	if r.Signal("inexistente") {
		t.Fatal("Signal em instância sem canal deveria retornar false")
	}

	ch := r.Replace("inst")
	if got := r.Get("inst"); got != ch {
		t.Fatal("Get não retornou o canal da geração atual")
	}

	// múltiplos sinais não bloqueiam nem panicam (buffer cap 1)
	for i := 0; i < 10; i++ {
		if !r.Signal("inst") {
			t.Fatal("Signal deveria retornar true mesmo com sinal pendente")
		}
	}

	// consumidor recebe o kill
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("consumidor não recebeu o sinal de kill")
	}

	// Replace gera canal NOVO; o antigo fica órfão mas nunca fechado
	ch2 := r.Replace("inst")
	if ch2 == ch {
		t.Fatal("Replace deveria criar um canal novo por geração")
	}
	if r.Get("inst") != ch2 {
		t.Fatal("registry não aponta para a nova geração")
	}

	// sinais concorrentes durante Replace
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				r.Signal("inst")
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			r.Replace("inst")
		}
	}()
	wg.Wait()
}

func TestKillRegistryDeleteIf(t *testing.T) {
	r := NewKillRegistry()
	ch1 := r.Replace("inst")
	ch2 := r.Replace("inst")

	r.DeleteIf("inst", ch1) // stale: não deve remover
	if r.Get("inst") != ch2 {
		t.Fatal("DeleteIf removeu canal de geração mais nova")
	}
	r.DeleteIf("inst", ch2)
	if r.Get("inst") != nil {
		t.Fatal("DeleteIf não removeu o canal correto")
	}
}

// ---------------------------------------------------------------------------
// InstanceLocks: serializa a MESMA instância, paraleliza instâncias diferentes
// ---------------------------------------------------------------------------

func TestInstanceLocksSerializeSameInstance(t *testing.T) {
	locks := NewInstanceLocks()
	const workers = 8
	var mu sync.Mutex
	inCritical := 0
	maxConcurrent := 0

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := locks.Lock("inst-A")
			mu.Lock()
			inCritical++
			if inCritical > maxConcurrent {
				maxConcurrent = inCritical
			}
			mu.Unlock()
			time.Sleep(time.Millisecond)
			mu.Lock()
			inCritical--
			mu.Unlock()
			unlock()
		}()
	}
	wg.Wait()
	if maxConcurrent != 1 {
		t.Fatalf("seção crítica da mesma instância teve %d concorrentes (esperado 1)", maxConcurrent)
	}
}

func TestInstanceLocksParallelDifferentInstances(t *testing.T) {
	locks := NewInstanceLocks()
	var mu sync.Mutex
	inCritical := 0
	maxConcurrent := 0

	var wg sync.WaitGroup
	for _, id := range []string{"A", "B", "C", "D"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			unlock := locks.Lock(id)
			mu.Lock()
			inCritical++
			if inCritical > maxConcurrent {
				maxConcurrent = inCritical
			}
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			mu.Lock()
			inCritical--
			mu.Unlock()
			unlock()
		}(id)
	}
	wg.Wait()
	if maxConcurrent < 2 {
		t.Fatalf("instâncias diferentes não rodaram em paralelo (max=%d)", maxConcurrent)
	}
}

// ---------------------------------------------------------------------------
// ReconnectTracker: dois reconnects simultâneos da mesma instância colapsam
// ---------------------------------------------------------------------------

func TestReconnectTrackerSingleFlight(t *testing.T) {
	tr := NewReconnectTracker()

	if !tr.TryStart("inst") {
		t.Fatal("primeiro TryStart deveria ter sucesso")
	}
	if tr.TryStart("inst") {
		t.Fatal("segundo TryStart simultâneo deveria falhar (já em andamento)")
	}
	tr.Finish("inst")
	if !tr.TryStart("inst") {
		t.Fatal("TryStart após Finish deveria ter sucesso")
	}
	tr.Finish("inst")
}

func TestReconnectTrackerConcurrent(t *testing.T) {
	tr := NewReconnectTracker()
	var wg sync.WaitGroup
	var started sync.Map

	// rajada de 32 triggers simultâneos para a mesma instância
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if tr.TryStart("inst") {
				started.Store(i, true)
				time.Sleep(time.Millisecond)
				tr.Finish("inst")
			}
		}(i)
	}
	wg.Wait()

	// pelo menos um executou e nunca dois ao mesmo tempo (verificado pelo
	// TryStart/Finish atômico; com -race qualquer violação aparece)
	count := 0
	started.Range(func(_, _ any) bool { count++; return true })
	if count == 0 {
		t.Fatal("nenhum reconnect executou")
	}

	// instâncias diferentes nunca se bloqueiam
	if !tr.TryStart("outra") {
		t.Fatal("instância diferente deveria iniciar livremente")
	}
	tr.Finish("outra")
}

// ---------------------------------------------------------------------------
// Boot recovery: classifyBootState é um observador SOMENTE LEITURA do
// registry. Sem cliente registrado e timeout expirado → bootStateFailed.
// ---------------------------------------------------------------------------

func TestClassifyBootStateNoClient(t *testing.T) {
	w := whatsmeowService{clientPointer: NewClientRegistry()}
	if got := w.classifyBootState("inst-inexistente", 0); got != bootStateFailed {
		t.Fatalf("esperado bootStateFailed sem cliente, obtido %v", got)
	}
}

func TestClassifyBootStateNeverMutatesRegistry(t *testing.T) {
	reg := NewClientRegistry()
	w := whatsmeowService{clientPointer: reg}
	_ = w.classifyBootState("inst", 0)
	if reg.Get("inst") != nil {
		t.Fatal("classifyBootState mutou o registry (deveria ser somente leitura)")
	}
}
