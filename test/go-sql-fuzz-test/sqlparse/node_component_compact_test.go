package sqlparser

import "testing"

func TestAlterClusterStatement_StringToken(t *testing.T) {
	stmt, err := Parse("alter cluster 'k' 'v';")
	if err != nil {
		t.Fatalf("parse alter cluster failed: %v", err)
	}
	if _, ok := stmt.(*AlterClusterStmt); !ok {
		t.Fatalf("expected *AlterClusterStmt, got %T", stmt)
	}
}

func TestAlterLocalStatement_StringToken(t *testing.T) {
	stmt1, err := Parse("alter local 'k';")
	if err != nil {
		t.Fatalf("parse alter local failed: %v", err)
	}
	if _, ok := stmt1.(*AlterLocalStmt); !ok {
		t.Fatalf("expected *AlterLocalStmt, got %T", stmt1)
	}

	stmt2, err := Parse("alter local 'k' 'v';")
	if err != nil {
		t.Fatalf("parse alter local kv failed: %v", err)
	}
	if _, ok := stmt2.(*AlterLocalStmt); !ok {
		t.Fatalf("expected *AlterLocalStmt, got %T", stmt2)
	}
}

func TestAlterDnodesReloadStatement_Parse(t *testing.T) {
	stmt, err := Parse("alter dnodes reload tls;")
	if err != nil {
		t.Fatalf("parse alter dnodes reload failed: %v", err)
	}
	s, ok := stmt.(*AlterDnodesReloadStmt)
	if !ok {
		t.Fatalf("expected *AlterDnodesReloadStmt, got %T", stmt)
	}
	if s.Name != "tls" {
		t.Fatalf("unexpected reload name: %+v", s)
	}
}

func TestCreateAnodeStatement_StringToken(t *testing.T) {
	stmt, err := Parse("create anode 'ep';")
	if err != nil {
		t.Fatalf("parse create anode failed: %v", err)
	}
	if _, ok := stmt.(*CreateAnodeStmt); !ok {
		t.Fatalf("expected *CreateAnodeStmt, got %T", stmt)
	}
}

func TestUpdateAnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("update anode 1;")
	if err != nil {
		t.Fatalf("parse update anode failed: %v", err)
	}
	if _, ok := stmt.(*UpdateAnodeStmt); !ok {
		t.Fatalf("expected *UpdateAnodeStmt, got %T", stmt)
	}
}

func TestDropAnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("drop anode 1;")
	if err != nil {
		t.Fatalf("parse drop anode failed: %v", err)
	}
	if _, ok := stmt.(*DropAnodeStmt); !ok {
		t.Fatalf("expected *DropAnodeStmt, got %T", stmt)
	}
}

func TestUpdateAllAnodesStatement(t *testing.T) {
	stmt, err := Parse("update all anodes;")
	if err != nil {
		t.Fatalf("parse update all anodes failed: %v", err)
	}
	ua, ok := stmt.(*UpdateAnodeStmt)
	if !ok {
		t.Fatalf("expected *UpdateAnodeStmt, got %T", stmt)
	}
	if ua.AnodeId != -1 {
		t.Fatalf("expected update-all marker anode id -1, got %+v", ua)
	}
}

func TestCreateBnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("create bnode on dnode 1;")
	if err != nil {
		t.Fatalf("parse create bnode failed: %v", err)
	}
	if _, ok := stmt.(*CreateBnodeStmt); !ok {
		t.Fatalf("expected *CreateBnodeStmt, got %T", stmt)
	}
}

func TestDropBnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("drop bnode on dnode 1;")
	if err != nil {
		t.Fatalf("parse drop bnode failed: %v", err)
	}
	if _, ok := stmt.(*DropBnodeStmt); !ok {
		t.Fatalf("expected *DropBnodeStmt, got %T", stmt)
	}
}

func TestCreateMnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("create mnode on dnode 1;")
	if err != nil {
		t.Fatalf("parse create mnode failed: %v", err)
	}
	if _, ok := stmt.(*CreateComponentNodeStmt); !ok {
		t.Fatalf("expected *CreateComponentNodeStmt, got %T", stmt)
	}
}

func TestDropMnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("drop mnode on dnode 1;")
	if err != nil {
		t.Fatalf("parse drop mnode failed: %v", err)
	}
	if _, ok := stmt.(*DropComponentNodeStmt); !ok {
		t.Fatalf("expected *DropComponentNodeStmt, got %T", stmt)
	}
}

func TestRestoreMnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("restore mnode on dnode 1;")
	if err != nil {
		t.Fatalf("parse restore mnode failed: %v", err)
	}
	if _, ok := stmt.(*RestoreComponentNodeStmt); !ok {
		t.Fatalf("expected *RestoreComponentNodeStmt, got %T", stmt)
	}
}

func TestCreateQnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("create qnode on dnode 1;")
	if err != nil {
		t.Fatalf("parse create qnode failed: %v", err)
	}
	if _, ok := stmt.(*CreateComponentNodeStmt); !ok {
		t.Fatalf("expected *CreateComponentNodeStmt, got %T", stmt)
	}
}

func TestDropQnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("drop qnode on dnode 1;")
	if err != nil {
		t.Fatalf("parse drop qnode failed: %v", err)
	}
	if _, ok := stmt.(*DropComponentNodeStmt); !ok {
		t.Fatalf("expected *DropComponentNodeStmt, got %T", stmt)
	}
}

func TestRestoreQnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("restore qnode on dnode 1;")
	if err != nil {
		t.Fatalf("parse restore qnode failed: %v", err)
	}
	if _, ok := stmt.(*RestoreComponentNodeStmt); !ok {
		t.Fatalf("expected *RestoreComponentNodeStmt, got %T", stmt)
	}
}

func TestCreateSnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("create snode on dnode 1;")
	if err != nil {
		t.Fatalf("parse create snode failed: %v", err)
	}
	if _, ok := stmt.(*CreateComponentNodeStmt); !ok {
		t.Fatalf("expected *CreateComponentNodeStmt, got %T", stmt)
	}
}

func TestDropSnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("drop snode on dnode 1;")
	if err != nil {
		t.Fatalf("parse drop snode failed: %v", err)
	}
	if _, ok := stmt.(*DropComponentNodeStmt); !ok {
		t.Fatalf("expected *DropComponentNodeStmt, got %T", stmt)
	}
}

func TestRestoreVnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("restore vnode on dnode 1;")
	if err != nil {
		t.Fatalf("parse restore vnode failed: %v", err)
	}
	if _, ok := stmt.(*RestoreComponentNodeStmt); !ok {
		t.Fatalf("expected *RestoreComponentNodeStmt, got %T", stmt)
	}
}

func TestCreateDnodeStatement_QuotedEndpoint(t *testing.T) {
	stmt, err := Parse("create dnode 'n1';")
	if err != nil {
		t.Fatalf("parse create dnode failed: %v", err)
	}
	if _, ok := stmt.(*CreateDnodeStmt); !ok {
		t.Fatalf("expected *CreateDnodeStmt, got %T", stmt)
	}
}

func TestCreateDnodeStatement_PortIntegerToken(t *testing.T) {
	stmt, err := Parse("create dnode 'n1' port 6030;")
	if err != nil {
		t.Fatalf("parse create dnode with port failed: %v", err)
	}
	if _, ok := stmt.(*CreateDnodeStmt); !ok {
		t.Fatalf("expected *CreateDnodeStmt, got %T", stmt)
	}
}

func TestRestoreDnodeStatement_IntegerToken(t *testing.T) {
	stmt, err := Parse("restore dnode 1;")
	if err != nil {
		t.Fatalf("parse restore dnode failed: %v", err)
	}
	if _, ok := stmt.(*RestoreDnodeStmt); !ok {
		t.Fatalf("expected *RestoreDnodeStmt, got %T", stmt)
	}
}

func TestAlterDnodeStatement_IntegerToken(t *testing.T) {
	stmt1, err := Parse("alter dnode 1 'cfg';")
	if err != nil {
		t.Fatalf("parse alter dnode simple failed: %v", err)
	}
	if _, ok := stmt1.(*AlterDnodeStmt); !ok {
		t.Fatalf("expected *AlterDnodeStmt, got %T", stmt1)
	}

	stmt2, err := Parse("alter dnode 1 'cfg' 'v';")
	if err != nil {
		t.Fatalf("parse alter dnode kv failed: %v", err)
	}
	if _, ok := stmt2.(*AlterDnodeStmt); !ok {
		t.Fatalf("expected *AlterDnodeStmt, got %T", stmt2)
	}
}

func TestAlterAllDnodesStatement_StringToken(t *testing.T) {
	stmt1, err := Parse("alter all dnodes 'k';")
	if err != nil {
		t.Fatalf("parse alter all dnodes failed: %v", err)
	}
	if _, ok := stmt1.(*AlterDnodeStmt); !ok {
		t.Fatalf("expected *AlterDnodeStmt, got %T", stmt1)
	}

	stmt2, err := Parse("alter all dnodes 'k' 'v';")
	if err != nil {
		t.Fatalf("parse alter all dnodes kv failed: %v", err)
	}
	if _, ok := stmt2.(*AlterDnodeStmt); !ok {
		t.Fatalf("expected *AlterDnodeStmt, got %T", stmt2)
	}
}

func TestDropDnodeStatement_OptionVariants(t *testing.T) {
	d0 := NewDropDnodeStmt(nil, Token{Type: ID, Bytes: []byte("node1")}, false, false)
	if d0.Fqdn != "node1" || d0.Force || d0.Unsafe {
		t.Fatalf("unexpected default drop dnode stmt: %+v", d0)
	}

	d1 := NewDropDnodeStmt(nil, Token{Type: ID, Bytes: []byte("node1")}, true, false)
	if !d1.Force || d1.Unsafe {
		t.Fatalf("unexpected force flags: %+v", d1)
	}

	d2 := NewDropDnodeStmt(nil, Token{Type: ID, Bytes: []byte("node1")}, false, true)
	if d2.Force || !d2.Unsafe {
		t.Fatalf("unexpected unsafe flags: %+v", d2)
	}
}

func TestDropDnodeStatement_ParseVariants(t *testing.T) {
	stmt0, err := Parse("drop dnode node1;")
	if err != nil {
		t.Fatalf("parse drop dnode default failed: %v", err)
	}
	d0, ok := stmt0.(*DropDnodeStmt)
	if !ok {
		t.Fatalf("expected *DropDnodeStmt, got %T", stmt0)
	}
	if d0.Fqdn != "node1" || d0.Force || d0.Unsafe {
		t.Fatalf("unexpected parsed default stmt: %+v", d0)
	}

	stmt1, err := Parse("drop dnode node1 unsafe;")
	if err != nil {
		t.Fatalf("parse drop dnode unsafe failed: %v", err)
	}
	d1, ok := stmt1.(*DropDnodeStmt)
	if !ok {
		t.Fatalf("expected *DropDnodeStmt, got %T", stmt1)
	}
	if d1.Fqdn != "node1" || d1.Force || !d1.Unsafe {
		t.Fatalf("unexpected parsed unsafe stmt: %+v", d1)
	}

	stmt2, err := Parse("drop dnode 1 force;")
	if err != nil {
		t.Fatalf("parse drop dnode force failed: %v", err)
	}
	d2, ok := stmt2.(*DropDnodeStmt)
	if !ok {
		t.Fatalf("expected *DropDnodeStmt, got %T", stmt2)
	}
	if d2.DnodeId != 1 || !d2.Force || d2.Unsafe {
		t.Fatalf("unexpected parsed force stmt: %+v", d2)
	}
}
