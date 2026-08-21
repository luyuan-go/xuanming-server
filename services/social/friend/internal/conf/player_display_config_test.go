package conf

import "testing"

func TestValidatePlayerDisplayResolvers(t *testing.T) {
	cfg := Config{Friend: FriendConf{
		PlayerNameResolverAddr:         "player:20002",
		PlayerNameResolverAuthSecret:   "friend-player-name-key-0123456789abcdef",
		PlayerNameResolverAuthAudience: "player:name",
		PlayerNoResolverAddr:           "login:20001",
		PlayerNoResolverAuthSecret:     "friend-player-no-key-0123456789abcdef00",
		PlayerNoResolverAuthAudience:   "login:player-no",
	}}
	if err := cfg.ValidatePlayerDisplayResolvers(); err != nil {
		t.Fatalf("valid display resolver config: %v", err)
	}

	cfg.Friend.PlayerNoResolverAuthSecret = cfg.Friend.PlayerNameResolverAuthSecret
	if err := cfg.ValidatePlayerDisplayResolvers(); err == nil {
		t.Fatal("reusing one HMAC key across name and player_no authorities must fail")
	}

	cfg.Friend.PlayerNoResolverAuthSecret = ""
	if err := cfg.ValidatePlayerDisplayResolvers(); err == nil {
		t.Fatal("configured resolver address without secret must fail")
	}
}
