package whatsmeow_service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	config "github.com/evolution-foundation/evolution-go/pkg/config"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

func names(s []string) map[string]bool {
	m := map[string]bool{}
	for _, n := range s { m[n] = true }
	return m
}

func TestFilterBootCandidates(t *testing.T) {
	allow := map[string]struct{}{"ativo-rosana": {}, "ativo-jessica": {}}
	rec, skipped := filterBootCandidates(
		[]string{"ativo-rosana", "ativo-helena-queren", "teste-ativo", "ativo-jessica"}, allow)
	got := names(rec)
	if !got["ativo-rosana"] || !got["ativo-jessica"] || len(rec) != 2 {
		t.Fatalf("recover incorreto: %v", rec)
	}
	if skipped["ativo-helena-queren"] != "SKIPPED_NOT_CONFIRMED_OPERATIONAL" {
		t.Fatalf("antiga deveria ser SKIPPED_NOT_CONFIRMED_OPERATIONAL: %v", skipped)
	}
	if skipped["teste-ativo"] != "SKIPPED_TEST" {
		t.Fatalf("teste deveria ser SKIPPED_TEST: %v", skipped)
	}
}

func TestFilterBootCandidatesEmptyAllowlistSkipsEverything(t *testing.T) {
	rec, skipped := filterBootCandidates([]string{"ativo-rosana"}, map[string]struct{}{})
	if len(rec) != 0 || skipped["ativo-rosana"] != "SKIPPED_NOT_CONFIRMED_OPERATIONAL" {
		t.Fatalf("fail-closed quebrado: rec=%v skipped=%v", rec, skipped)
	}
}

func testService(cfg *config.Config) whatsmeowService {
	cfg.LogDirectory = "/tmp/evolution-go-test-logs"
	return whatsmeowService{config: cfg, loggerWrapper: logger_wrapper.NewLoggerManager(cfg)}
}

func TestResolveBootAllowlistEnvPrecedence(t *testing.T) {
	// servidor que NÃO deveria ser chamado quando env tem prioridade
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	defer srv.Close()
	w := testService(&config.Config{
		StartupRecoveryAllowlist: []string{"ativo-rosana"},
		DashoneBootAllowlistUrl:  srv.URL,
		DashoneBootAllowlistKey:  "k",
	})
	allow, source := w.resolveBootAllowlist("test")
	if source != "env" || called {
		t.Fatalf("env deveria ter precedencia sem chamar endpoint: source=%s called=%v", source, called)
	}
	if _, ok := allow["ativo-rosana"]; !ok {
		t.Fatal("allowlist env nao aplicada")
	}
}

func TestResolveBootAllowlistDashoneEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-allowlist-key") != "segredo" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"operational": []string{"ativo-carla", "ativo-jessica"}})
	}))
	defer srv.Close()
	w := testService(&config.Config{DashoneBootAllowlistUrl: srv.URL, DashoneBootAllowlistKey: "segredo"})
	allow, source := w.resolveBootAllowlist("test")
	if source != "dashone" || len(allow) != 2 {
		t.Fatalf("endpoint nao aplicado: source=%s allow=%v", source, allow)
	}
}

func TestResolveBootAllowlistFailClosed(t *testing.T) {
	// sem nenhuma fonte configurada
	w := testService(&config.Config{})
	allow, source := w.resolveBootAllowlist("test")
	if source != "none" || len(allow) != 0 {
		t.Fatalf("deveria falhar fechado: source=%s allow=%v", source, allow)
	}
	// endpoint recusando a chave
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	defer srv.Close()
	w2 := testService(&config.Config{DashoneBootAllowlistUrl: srv.URL, DashoneBootAllowlistKey: "errada"})
	allow2, source2 := w2.resolveBootAllowlist("test")
	if source2 != "none" || len(allow2) != 0 {
		t.Fatalf("401 deveria falhar fechado: source=%s allow=%v", source2, allow2)
	}
	// endpoint fora do ar
	w3 := testService(&config.Config{DashoneBootAllowlistUrl: "http://127.0.0.1:1", DashoneBootAllowlistKey: "k"})
	allow3, _ := w3.resolveBootAllowlist("test")
	if len(allow3) != 0 {
		t.Fatal("endpoint fora do ar deveria falhar fechado")
	}
}
