package sqlparser

import "testing"

func TestShowXnodesStatement_Parse(t *testing.T) {
	stmt, err := Parse("show xnodes;")
	if err != nil {
		t.Fatalf("parse show xnodes failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "xnodes" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowDnodesStatement_Parse(t *testing.T) {
	stmt, err := Parse("show dnodes;")
	if err != nil {
		t.Fatalf("parse show dnodes failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "dnodes" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowUsersStatement_Parse(t *testing.T) {
	stmt, err := Parse("show users;")
	if err != nil {
		t.Fatalf("parse show users failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "users" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowUsersFullStatement_Parse(t *testing.T) {
	stmt, err := Parse("show users full;")
	if err != nil {
		t.Fatalf("parse show users full failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "users_full" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowUserPrivilegesStatement_Parse(t *testing.T) {
	stmt, err := Parse("show user privileges;")
	if err != nil {
		t.Fatalf("parse show user privileges failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "user_privileges" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowRolesStatement_Parse(t *testing.T) {
	stmt, err := Parse("show roles;")
	if err != nil {
		t.Fatalf("parse show roles failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "roles" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowRolePrivilegesStatement_Parse(t *testing.T) {
	stmt, err := Parse("show role privileges;")
	if err != nil {
		t.Fatalf("parse show role privileges failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "role_privileges" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowRoleColumnPrivilegesStatement_Parse(t *testing.T) {
	stmt, err := Parse("show role column privileges;")
	if err != nil {
		t.Fatalf("parse show role column privileges failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "role_column_privileges" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowAppsStatement_Parse(t *testing.T) {
	stmt, err := Parse("show apps;")
	if err != nil {
		t.Fatalf("parse show apps failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "apps" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowConnectionsStatement_Parse(t *testing.T) {
	stmt, err := Parse("show connections;")
	if err != nil {
		t.Fatalf("parse show connections failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "connections" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowLicencesStatement_Parse(t *testing.T) {
	stmt, err := Parse("show licences;")
	if err != nil {
		t.Fatalf("parse show licences failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "licences" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowGrantsStatement_Parse(t *testing.T) {
	stmt, err := Parse("show grants;")
	if err != nil {
		t.Fatalf("parse show grants failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "grants" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowGrantsFullStatement_Parse(t *testing.T) {
	stmt, err := Parse("show grants full;")
	if err != nil {
		t.Fatalf("parse show grants full failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "grants_full" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowGrantsLogsStatement_Parse(t *testing.T) {
	stmt, err := Parse("show grants logs;")
	if err != nil {
		t.Fatalf("parse show grants logs failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "grants_logs" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowEncryptionsStatement_Parse(t *testing.T) {
	stmt, err := Parse("show encryptions;")
	if err != nil {
		t.Fatalf("parse show encryptions failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "encryptions" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowEncryptAlgorithmsStatement_Parse(t *testing.T) {
	stmt, err := Parse("show encrypt_algorithms;")
	if err != nil {
		t.Fatalf("parse show encrypt_algorithms failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "encrypt_algorithms" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowEncryptStatusStatement_Parse(t *testing.T) {
	stmt, err := Parse("show encrypt_status;")
	if err != nil {
		t.Fatalf("parse show encrypt_status failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "encrypt_status" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowQueriesStatement_Parse(t *testing.T) {
	stmt, err := Parse("show queries;")
	if err != nil {
		t.Fatalf("parse show queries failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "queries" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowScoresStatement_Parse(t *testing.T) {
	stmt, err := Parse("show scores;")
	if err != nil {
		t.Fatalf("parse show scores failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "scores" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowTopicsStatement_Parse(t *testing.T) {
	stmt, err := Parse("show topics;")
	if err != nil {
		t.Fatalf("parse show topics failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "topics" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowConsumersStatement_Parse(t *testing.T) {
	stmt, err := Parse("show consumers;")
	if err != nil {
		t.Fatalf("parse show consumers failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "consumers" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowSubscriptionsStatement_Parse(t *testing.T) {
	stmt, err := Parse("show subscriptions;")
	if err != nil {
		t.Fatalf("parse show subscriptions failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "subscriptions" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowTokensStatement_Parse(t *testing.T) {
	stmt, err := Parse("show tokens;")
	if err != nil {
		t.Fatalf("parse show tokens failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "tokens" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowSnodesStatement_Parse(t *testing.T) {
	stmt, err := Parse("show snodes;")
	if err != nil {
		t.Fatalf("parse show snodes failed: %v", err)
	}
	s, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}
	if s.Kind != "snodes" {
		t.Fatalf("unexpected show stmt: %+v", s)
	}
}

func TestShowUserPrivilegesStatement_ParseError(t *testing.T) {
	if _, err := Parse("show privileges user;"); err == nil {
		t.Fatalf("expected parse error for invalid keyword order")
	}
}

func TestShowRoleColumnPrivilegesStatement_ParseError(t *testing.T) {
	if _, err := Parse("show role privileges column;"); err == nil {
		t.Fatalf("expected parse error for invalid keyword order")
	}
}

func TestShowAppsStatement_ParseError(t *testing.T) {
	if _, err := Parse("show app;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowConnectionsStatement_ParseError(t *testing.T) {
	if _, err := Parse("show connection;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowLicencesStatement_ParseError(t *testing.T) {
	if _, err := Parse("show licence;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowGrantsStatement_ParseError(t *testing.T) {
	if _, err := Parse("show grant;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowGrantsFullStatement_ParseError(t *testing.T) {
	if _, err := Parse("show grantsful;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowGrantsLogsStatement_ParseError(t *testing.T) {
	if _, err := Parse("show grants log;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowEncryptionsStatement_ParseError(t *testing.T) {
	if _, err := Parse("show encryption;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowEncryptAlgorithmsStatement_ParseError(t *testing.T) {
	if _, err := Parse("show encrypt_algorithm;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowEncryptStatusStatement_ParseError(t *testing.T) {
	if _, err := Parse("show encryptstate;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowQueriesStatement_ParseError(t *testing.T) {
	if _, err := Parse("show query;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowScoresStatement_ParseError(t *testing.T) {
	if _, err := Parse("show score;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowTopicsStatement_ParseError(t *testing.T) {
	if _, err := Parse("show topic;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowConsumersStatement_ParseError(t *testing.T) {
	if _, err := Parse("show consumer;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowSubscriptionsStatement_ParseError(t *testing.T) {
	if _, err := Parse("show subscription;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowTokensStatement_ParseError(t *testing.T) {
	if _, err := Parse("show token;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}

func TestShowSnodesStatement_ParseError(t *testing.T) {
	if _, err := Parse("show snode;"); err == nil {
		t.Fatalf("expected parse error for unknown keyword")
	}
}
