package main

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestBatch100Addresses(t *testing.T) {
	const count = 100
	wallets := make([]WalletItem, 0, count)
	addrs := make([]string, 0, count)

	for i := 0; i < count; i++ {
		mnemonic, err := generateMnemonic()
		if err != nil {
			t.Fatalf("Failed to generate mnemonic: %v", err)
		}
		addr, err := deriveSolanaAddress(mnemonic, "")
		if err != nil {
			t.Fatalf("Failed to derive address: %v", err)
		}
		wallets = append(wallets, WalletItem{Mnemonic: mnemonic, Address: addr})
		addrs = append(addrs, addr)
	}

	rpcMgr := NewRPCManager("dad6493c-fff4-4280-8a90-857fdf98c1b3", nil, 30*time.Second)
	client := &http.Client{Timeout: 10 * time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	accounts, rpcURL, err := checkMultipleAccountsWithAutoRotation(ctx, client, rpcMgr, addrs, 3, 5*time.Second)
	if err != nil {
		t.Fatalf("checkMultipleAccountsWithAutoRotation failed: %v", err)
	}

	if len(accounts) != count {
		t.Fatalf("Expected %d accounts in response, got %d", count, len(accounts))
	}

	t.Logf("Successfully checked %d addresses in ONE RPC request using %s", count, cleanRPCName(rpcURL))
}

func TestMainnetBetaSolanaRPC(t *testing.T) {
	// Directly test https://api.mainnet-beta.solana.com
	rpcMgr := &RPCManager{
		endpoints: []*RPCEndpoint{
			{URL: "https://api.mainnet-beta.solana.com"},
		},
		cooldown: 30 * time.Second,
	}
	client := &http.Client{Timeout: 10 * time.Second}

	knownAddrs := []string{
		"11111111111111111111111111111111",
		"So11111111111111111111111111111111111111112",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	accounts, rpcURL, err := checkMultipleAccountsWithAutoRotation(ctx, client, rpcMgr, knownAddrs, 3, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to query https://api.mainnet-beta.solana.com: %v", err)
	}

	if len(accounts) != len(knownAddrs) {
		t.Fatalf("Expected %d accounts, got %d", len(knownAddrs), len(accounts))
	}

	t.Logf("Successfully queried %s (%s)", rpcURL, cleanRPCName(rpcURL))
}
