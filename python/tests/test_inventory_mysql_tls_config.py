from __future__ import annotations

from pandorapy.services.inventory.conf import BagConf


def test_bag_conf_retains_central_mysql_tls_and_pool_fields() -> None:
    conf = BagConf(
        dsn="planner@tcp(db.intra:3306)/pandora_bag",
        tls_ca_file=r"C:\ProgramData\Pandora\ca\planner-db-ca.pem",
        tls_server_name="db.intra",
        max_open_conns=4,
        max_idle_conns=1,
        conn_max_lifetime="30m",
        conn_max_idle_time="5m",
        ping_timeout="3s",
    )

    assert conf.tls_ca_file == r"C:\ProgramData\Pandora\ca\planner-db-ca.pem"
    assert conf.tls_server_name == "db.intra"
    assert conf.max_open_conns == 4
    assert conf.max_idle_conns == 1
    assert conf.conn_max_lifetime == "30m"
    assert conf.conn_max_idle_time == "5m"
    assert conf.ping_timeout == "3s"
