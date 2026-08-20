package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	crand "crypto/rand"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mr-tron/base58"
	bip39 "github.com/tyler-smith/go-bip39"
)

// ---------------- Configuration ----------------
type Config struct {
	TelegramBotToken string
	TelegramChatID   string
	HeliusApiKey     string
	Concurrency      int
	DelayMs          int
	RPCTimeout       time.Duration
	Port             string
	CustomRPCs       []string
}

func loadConfig() Config {
	concurrency, _ := strconv.Atoi(getEnv("CONCURRENCY", "4"))
	if concurrency < 1 {
		concurrency = 1
	}
	delayMs, _ := strconv.Atoi(getEnv("DELAY_MS", "200"))
	if delayMs < 0 {
		delayMs = 0
	}
	rpcTimeoutSec, _ := strconv.Atoi(getEnv("RPC_TIMEOUT_SEC", "5"))
	if rpcTimeoutSec < 1 {
		rpcTimeoutSec = 5
	}

	port := getEnv("PORT", "8080")
	heliusKey := getEnv("HELIUS_API_KEY", "dad6493c-fff4-4280-8a90-857fdf98c1b3")

	var customRPCs []string
	if rawRPCs := os.Getenv("CUSTOM_RPCS"); rawRPCs != "" {
		for _, r := range strings.Split(rawRPCs, ",") {
			r = strings.TrimSpace(r)
			if r != "" {
				customRPCs = append(customRPCs, r)
			}
		}
	}

	return Config{
		TelegramBotToken: os.Getenv("TELEGRAM_BOT_TOKEN"),
		TelegramChatID:   os.Getenv("TELEGRAM_CHAT_ID"),
		HeliusApiKey:     heliusKey,
		Concurrency:      concurrency,
		DelayMs:          delayMs,
		RPCTimeout:       time.Duration(rpcTimeoutSec) * time.Second,
		Port:             port,
		CustomRPCs:       customRPCs,
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// ---------------- Metrics & State ----------------
var (
	startTime    = time.Now()
	totalChecked uint64
	foundCount   uint64
)

// ---------------- Telegram ----------------
func sendTelegramNotification(token, chatID, msg string) {
	if token == "" || chatID == "" {
		return
	}
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token)
	payload := map[string]string{
		"chat_id": chatID,
		"text":    msg,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(data))
	if err == nil {
		defer resp.Body.Close()
	}
}

// ---------------- Smart RPC Pool with Auto-Rotation & Circuit Breaker ----------------

type RPCEndpoint struct {
	URL           string    `json:"url"`
	CooldownUntil time.Time `json:"cooldown_until"`
	FailCount     int       `json:"fail_count"`
	SuccessCount  uint64    `json:"success_count"`
	LastError     string    `json:"last_error,omitempty"`
}

type RPCManager struct {
	mu           sync.RWMutex
	endpoints    []*RPCEndpoint
	currentIndex uint64
	cooldown     time.Duration
}

var defaultRPCs = []string{
	"https://api.mainnet-beta.solana.com",
	"https://solana-rpc.publicnode.com",
	"https://solana.lava.build",
	"https://api.mainnet.solana.com",
}

func NewRPCManager(heliusKey string, customRPCs []string, cooldown time.Duration) *RPCManager {
	seen := make(map[string]bool)
	var endpoints []*RPCEndpoint

	var priorityRPCs []string
	if heliusKey != "" {
		priorityRPCs = append(priorityRPCs,
			fmt.Sprintf("https://mainnet.helius-rpc.com/?api-key=%s", heliusKey),
			fmt.Sprintf("https://rpc.helius.xyz/?api-key=%s", heliusKey),
		)
	}

	all := append(priorityRPCs, append(customRPCs, defaultRPCs...)...)
	for _, raw := range all {
		u := strings.TrimSpace(raw)
		if u != "" && !seen[u] {
			seen[u] = true
			endpoints = append(endpoints, &RPCEndpoint{
				URL: u,
			})
		}
	}

	return &RPCManager{
		endpoints: endpoints,
		cooldown:  cooldown,
	}
}

// GetNextHealthyRPC selects the next available healthy RPC via round-robin.
// If all endpoints are cooling down, it returns the endpoint with the shortest remaining cooldown.
func (m *RPCManager) GetNextHealthyRPC() *RPCEndpoint {
	m.mu.Lock()
	defer m.mu.Unlock()

	n := len(m.endpoints)
	if n == 0 {
		return nil
	}

	now := time.Now()
	startIdx := atomic.AddUint64(&m.currentIndex, 1)

	// 1. Try to find a healthy endpoint not in cooldown
	for i := 0; i < n; i++ {
		idx := int((startIdx + uint64(i)) % uint64(n))
		ep := m.endpoints[idx]
		if now.After(ep.CooldownUntil) {
			return ep
		}
	}

	// 2. If all are in cooldown, pick the one that recovers earliest
	best := m.endpoints[0]
	for _, ep := range m.endpoints[1:] {
		if ep.CooldownUntil.Before(best.CooldownUntil) {
			best = ep
		}
	}
	return best
}

func (m *RPCManager) MarkSuccess(url string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ep := range m.endpoints {
		if ep.URL == url {
			ep.FailCount = 0
			ep.SuccessCount++
			ep.LastError = ""
			break
		}
	}
}

func (m *RPCManager) MarkRateLimited(url string, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ep := range m.endpoints {
		if ep.URL == url {
			ep.FailCount++
			// Exponential backoff: 30s, 60s, 120s... max 5 mins
			backoffMultiplier := 1 << (ep.FailCount - 1)
			if backoffMultiplier > 10 {
				backoffMultiplier = 10
			}
			duration := m.cooldown * time.Duration(backoffMultiplier)
			ep.CooldownUntil = time.Now().Add(duration)
			ep.LastError = reason
			log.Printf("[RPC Auto-Rotation] Endpoint rate-limited/failed: %s (cooldown for %v). Reason: %s\n", url, duration, reason)
			break
		}
	}
}

func (m *RPCManager) Status() []map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	var list []map[string]interface{}
	for _, ep := range m.endpoints {
		isCooling := now.Before(ep.CooldownUntil)
		remainingSec := 0
		if isCooling {
			remainingSec = int(time.Until(ep.CooldownUntil).Seconds())
		}
		list = append(list, map[string]interface{}{
			"url":           ep.URL,
			"is_healthy":    !isCooling,
			"cooldown_left": fmt.Sprintf("%ds", remainingSec),
			"success_count": ep.SuccessCount,
			"fail_count":    ep.FailCount,
			"last_error":    ep.LastError,
		})
	}
	return list
}

// ---------------- Key Derivation (Solana BIP44) ----------------
func deriveMaster(seed []byte) (k, c []byte) {
	mac := hmac.New(sha512.New, []byte("ed25519 seed"))
	mac.Write(seed)
	I := mac.Sum(nil)
	return I[:32], I[32:]
}

func deriveChildHardened(parentKey, parentChain []byte, index uint32) (childKey, childChain []byte) {
	data := make([]byte, 1+32+4)
	data[0] = 0x00
	copy(data[1:33], parentKey)
	data[33] = byte((index >> 24) & 0xff)
	data[34] = byte((index >> 16) & 0xff)
	data[35] = byte((index >> 8) & 0xff)
	data[36] = byte(index & 0xff)

	mac := hmac.New(sha512.New, parentChain)
	mac.Write(data)
	I := mac.Sum(nil)
	return I[:32], I[32:]
}

func deriveSolanaAddress(mnemonic, passphrase string) (string, error) {
	seed := bip39.NewSeed(mnemonic, passphrase)
	k, c := deriveMaster(seed)
	k, c = deriveChildHardened(k, c, 44|0x80000000)
	k, c = deriveChildHardened(k, c, 501|0x80000000)
	k, c = deriveChildHardened(k, c, 0|0x80000000)
	k, _ = deriveChildHardened(k, c, 0|0x80000000)

	priv := ed25519.NewKeyFromSeed(k)
	pub := priv[32:]
	return base58.Encode(pub), nil
}

func generateMnemonic() (string, error) {
	entropy := make([]byte, 16)
	if _, err := crand.Read(entropy); err != nil {
		return "", err
	}
	return bip39.NewMnemonic(entropy)
}

// ---------------- Resilient Balance Check with Auto-Rotation & Fallback ----------------
func checkBalanceWithAutoRotation(ctx context.Context, client *http.Client, rpcMgr *RPCManager, addr string, maxRetries int, timeout time.Duration) (float64, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getBalance",
		"params":  []interface{}{addr},
	}
	bodyBytes, _ := json.Marshal(payload)

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		ep := rpcMgr.GetNextHealthyRPC()
		if ep == nil {
			return 0, fmt.Errorf("no RPC endpoints available")
		}

		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		req, err := http.NewRequestWithContext(reqCtx, "POST", ep.URL, bytes.NewReader(bodyBytes))
		if err != nil {
			cancel()
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			cancel()
			rpcMgr.MarkRateLimited(ep.URL, fmt.Sprintf("network/timeout: %v", err))
			continue
		}

		// Handle HTTP Rate Limit or Server Errors
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusForbidden || resp.StatusCode >= 500 {
			resp.Body.Close()
			cancel()
			rpcMgr.MarkRateLimited(ep.URL, fmt.Sprintf("HTTP %d", resp.StatusCode))
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()

		if err != nil {
			rpcMgr.MarkRateLimited(ep.URL, fmt.Sprintf("read error: %v", err))
			continue
		}

		var res struct {
			Result struct {
				Value int64 `json:"value"`
			} `json:"result"`
			Error *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}

		if err := json.Unmarshal(body, &res); err != nil {
			rpcMgr.MarkRateLimited(ep.URL, "invalid JSON response")
			continue
		}

		if res.Error != nil {
			// RPC rate-limit error code (-32005 or similar)
			rpcMgr.MarkRateLimited(ep.URL, fmt.Sprintf("rpc error [%d]: %s", res.Error.Code, res.Error.Message))
			continue
		}

		// Success!
		rpcMgr.MarkSuccess(ep.URL)
		return float64(res.Result.Value) / 1e9, nil
	}

	return 0, fmt.Errorf("all RPC attempts exhausted")
}

// ---------------- Main ----------------
func main() {
	cfg := loadConfig()
	client := &http.Client{}

	// Initialize RPC Manager with 30-second initial cooldown
	rpcMgr := NewRPCManager(cfg.HeliusApiKey, cfg.CustomRPCs, 30*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Health check & status server (Required by Railway / Render / Fly.io)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		status := map[string]interface{}{
			"status":        "healthy",
			"uptime":        time.Since(startTime).String(),
			"total_checked": atomic.LoadUint64(&totalChecked),
			"found":         atomic.LoadUint64(&foundCount),
			"concurrency":   cfg.Concurrency,
			"rpc_pool":      rpcMgr.Status(),
		}
		json.NewEncoder(w).Encode(status)
	})

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: mux,
	}

	go func() {
		log.Printf("Starting HTTP health & status server on port :%s\n", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server warning: %v\n", err)
		}
	}()

	// 2. Handle graceful termination
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received termination signal, shutting down...")
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("Worker loop started with %d RPC endpoints (Concurrency: %d, Delay: %dms)\n",
		len(rpcMgr.endpoints), cfg.Concurrency, cfg.DelayMs)

	// 3. Worker loop
	for {
		select {
		case <-ctx.Done():
			log.Println("Worker terminated.")
			return
		default:
		}

		var wg sync.WaitGroup
		for i := 0; i < cfg.Concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				mnemonic, err := generateMnemonic()
				if err != nil {
					return
				}
				addr, err := deriveSolanaAddress(mnemonic, "")
				if err != nil {
					return
				}

				// Check balance with auto-rotation across RPCs and up to 3 retries
				bal, err := checkBalanceWithAutoRotation(ctx, client, rpcMgr, addr, 3, cfg.RPCTimeout)
				if err != nil {
					return
				}

				atomic.AddUint64(&totalChecked, 1)

				if bal > 0 {
					atomic.AddUint64(&foundCount, 1)
					msg := fmt.Sprintf("🎉 Balance Found!\nAddress: %s\nBalance: %.9f SOL\nMnemonic: %s", addr, bal, mnemonic)
					log.Println(msg)

					// Log to local file
					f, err := os.OpenFile("nonzero.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					if err == nil {
						_, _ = f.WriteString(msg + "\n\n")
						f.Close()
					}

					// Send Telegram alert
					sendTelegramNotification(cfg.TelegramBotToken, cfg.TelegramChatID, msg)
				}
			}()
		}
		wg.Wait()

		if cfg.DelayMs > 0 {
			time.Sleep(time.Duration(cfg.DelayMs) * time.Millisecond)
		}
	}
}
