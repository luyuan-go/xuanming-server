package conf

import "testing"

func TestBagMySQLClientConfCarriesStrictTLSIdentity(t *testing.T) {
	t.Parallel()

	got := (BagConf{
		DSN:             "planner@tcp(db.intra:3306)/pandora_bag",
		TLSCAFile:       `C:\ProgramData\Pandora\ca\planner-db-ca.pem`,
		TLSServerName:   "db.intra",
		MaxOpenConns:    4,
		MaxIdleConns:    1,
		ConnMaxLifetime: 30,
		ConnMaxIdleTime: 31,
		PingTimeout:     32,
	}).MySQLClientConf()

	if got.DSN != "planner@tcp(db.intra:3306)/pandora_bag" ||
		got.TLSCAFile != `C:\ProgramData\Pandora\ca\planner-db-ca.pem` ||
		got.TLSServerName != "db.intra" || got.MaxOpenConns != 4 || got.MaxIdleConns != 1 ||
		got.ConnMaxLifetime != 30 || got.ConnMaxIdleTime != 31 || got.PingTimeout != 32 {
		t.Fatalf("MySQLClientConf() = %#v, 背包独立 DSN 丢失 TLS 身份", got)
	}
}
