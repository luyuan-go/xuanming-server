package plannerdb

import "testing"

func TestAdminDSNRequiresDedicatedVerifiedTLSAccountOnExactEndpoint(t *testing.T) {
	endpoint := Endpoint{Host: "pandora-dev-db.intra", Port: 3306, TLSServerName: "pandora-dev-db.intra"}
	valid := "planner_provisioner:secret@tcp(pandora-dev-db.intra:3306)/?tls=true"
	config, err := parseAdminDSN(valid, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if config.User != "planner_provisioner" || config.DBName != "" || config.TLSConfig != "true" {
		t.Fatalf("config=%+v", config)
	}

	invalid := []string{
		"root:secret@tcp(pandora-dev-db.intra:3306)/?tls=true",
		"planner:secret@tcp(pandora-dev-db.intra:3306)/pandora_account?tls=true",
		"planner:secret@tcp(pandora-dev-db.intra:3306)/",
		"planner:secret@tcp(other-db.intra:3306)/?tls=true",
		"planner:secret@tcp(192.168.2.5:3306)/?tls=true",
		"planner:secret@tcp(pandora-dev-db.intra:3306)/?tls=skip-verify",
	}
	for _, dsn := range invalid {
		if _, err := parseAdminDSN(dsn, endpoint); err == nil {
			t.Fatalf("DSN should fail: %s", dsn)
		}
	}
}

func TestRegistryIdentityRequiresSafeDatabaseAndExplicitInstanceMarker(t *testing.T) {
	valid := BackendConfig{
		RegistryDatabase:       "pandora_planner_registry",
		RegistryInstanceMarker: "planner-central-prod-01",
	}
	if err := validateRegistryIdentity(valid); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []BackendConfig{
		{RegistryDatabase: "pandora_planner_registry"},
		{RegistryDatabase: "pandora_planner_registry", RegistryInstanceMarker: " leading-space"},
		{RegistryDatabase: "pandora_planner_registry", RegistryInstanceMarker: "line\nbreak"},
		{RegistryDatabase: "mysql", RegistryInstanceMarker: "marker"},
		{RegistryDatabase: "pandora_planner_registry`; DROP DATABASE mysql; --", RegistryInstanceMarker: "marker"},
	} {
		if err := validateRegistryIdentity(invalid); err == nil {
			t.Fatalf("registry identity unexpectedly accepted: %+v", invalid)
		}
	}
}
