module github.com/luyuancpp/pandora/tools/plannerdb-provisioner

go 1.26.5

require (
	github.com/go-sql-driver/mysql v1.8.1
	github.com/luyuancpp/pandora/tools/migrate v0.0.0-00010101000000-000000000000
	golang.org/x/sys v0.45.0
)

require filippo.io/edwards25519 v1.1.0 // indirect

replace github.com/luyuancpp/pandora/tools/migrate => ../migrate
