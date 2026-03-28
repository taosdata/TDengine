package unified

import "testing"

// TestDSNDriverOpenConnectorDefaultPath verifies the expected behavior for this scenario.
func TestDSNDriverOpenConnectorDefaultPath(t *testing.T) {
	driver := NewDSNDriver("")
	connector, err := driver.OpenConnector("user:passwd@ws(127.0.0.1:6041)/db")
	if err != nil {
		t.Fatal(err)
	}
	cfg := connector.Config()
	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "ws://127.0.0.1:6041/ws" {
		t.Fatalf("unexpected endpoints: %v", cfg.Endpoints)
	}
}

// TestDSNDriverOpenConnectorCustomPath verifies the expected behavior for this scenario.
func TestDSNDriverOpenConnectorCustomPath(t *testing.T) {
	driver := NewDSNDriver("/custom-path")
	connector, err := driver.OpenConnector("user:passwd@ws(127.0.0.1:6041)/db")
	if err != nil {
		t.Fatal(err)
	}
	cfg := connector.Config()
	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "ws://127.0.0.1:6041/custom-path" {
		t.Fatalf("unexpected endpoints: %v", cfg.Endpoints)
	}
}

// TestPackageOpenConnectorUsesDefaultPath verifies the expected behavior for this scenario.
func TestPackageOpenConnectorUsesDefaultPath(t *testing.T) {
	connector, err := OpenConnector("user:passwd@ws(127.0.0.1:6041)/db")
	if err != nil {
		t.Fatal(err)
	}
	cfg := connector.Config()
	if len(cfg.Endpoints) != 1 || cfg.Endpoints[0] != "ws://127.0.0.1:6041/ws" {
		t.Fatalf("unexpected endpoints: %v", cfg.Endpoints)
	}
}
