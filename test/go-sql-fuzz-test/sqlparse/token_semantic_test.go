package sqlparser

import "testing"

func TestTokenOptions_Merge(t *testing.T) {
	base := &TokenOptions{Enable: 1, TTL: 0}
	left := &TokenOptions{HasEnable: true, Enable: 0, HasTTL: true, TTL: 86400}
	right := &TokenOptions{HasProvider: true, Provider: "p", HasExtraInfo: true, ExtraInfo: "e"}

	got := MergeTokenOptions(nil, MergeTokenOptions(nil, base, left), right)

	if got.Enable != 0 || got.TTL != 86400 || got.Provider != "p" || got.ExtraInfo != "e" {
		t.Fatalf("unexpected merged options: %+v", got)
	}
}

func TestCreateToken_TTLSemantics(t *testing.T) {
	stmt, err := Parse("create token tk1 from user u1 ttl 7;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	got, ok := stmt.(*CreateTokenStmt)
	if !ok {
		t.Fatalf("expected *CreateTokenStmt, got %T", stmt)
	}
	if got.TTL != 7*86400 {
		t.Fatalf("expected ttl in seconds %d, got %d", 7*86400, got.TTL)
	}
}

func TestCreateToken_EnableWithInteger(t *testing.T) {
	stmt, err := Parse("create token tk2 from user u1 enable 1;")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	got, ok := stmt.(*CreateTokenStmt)
	if !ok {
		t.Fatalf("expected *CreateTokenStmt, got %T", stmt)
	}
	if got.Enable != 1 {
		t.Fatalf("expected enable=1, got %d", got.Enable)
	}
}

func TestCreateToken_ProviderStringToken(t *testing.T) {
	stmt, err := Parse("create token tk3 from user u1 provider 'a';")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	got, ok := stmt.(*CreateTokenStmt)
	if !ok {
		t.Fatalf("expected *CreateTokenStmt, got %T", stmt)
	}
	if got.Provider != "a" {
		t.Fatalf("expected provider='a', got %+v", got)
	}
}

func TestCreateToken_ExtraInfoStringToken(t *testing.T) {
	stmt, err := Parse("create token tk4 from user u1 extra_info 'x';")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	got, ok := stmt.(*CreateTokenStmt)
	if !ok {
		t.Fatalf("expected *CreateTokenStmt, got %T", stmt)
	}
	if got.ExtraInfo != "x" {
		t.Fatalf("expected extra_info='x', got %+v", got)
	}
}
