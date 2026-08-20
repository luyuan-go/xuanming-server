"""`parse_go_dsn` 的解析契约。

★ 这条锁的是一个**静默连错库**的缺陷。

原实现是 `dsn.partition("@tcp(")` —— 对不含 `@tcp(` 的 DSN,partition 返回
`(dsn, "", "")`,于是 host 回落 `127.0.0.1`、port 回落 3306、db 变空串,
socket 路径被整个吞进 password 字段,**没有任何错误**。

实测(修复前):

    parse_go_dsn("u:p@unix(/var/run/mysqld.sock)/pandora_x")
    → {'host': '127.0.0.1', 'port': 3306, 'db': '', 'password': 'p@unix(/var/run/mysqld.sock)/pandora_x'}

这条路径上有 16 个服务的**全部** DSN(44 处调用)。配置写错 → 服务照常起来 →
连的却不是你以为的那个库。这正是本次迁移明确要抓的那类缺陷:写错了不报错。
"""

from __future__ import annotations

import pytest

from pandorapy.mysqlx import parse_go_dsn
from pandorapy.services.auction.shard_topology import shard_identity


# ── 正常形态 ────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    ("dsn", "want"),
    [
        (
            "u:p@tcp(db.internal:3307)/pandora_x",
            {"user": "u", "password": "p", "net": "tcp", "host": "db.internal", "port": 3307, "db": "pandora_x"},
        ),
        (
            # 没写端口 → 3306(Go 的 ParseDSN 同样回落)
            "u:p@tcp(db.internal)/pandora_x",
            {"user": "u", "password": "p", "net": "tcp", "host": "db.internal", "port": 3306, "db": "pandora_x"},
        ),
        (
            # 没写 host → 127.0.0.1(Go 同样)
            "u:p@tcp(:3306)/pandora_x",
            {"user": "u", "password": "p", "net": "tcp", "host": "127.0.0.1", "port": 3306, "db": "pandora_x"},
        ),
        (
            # 空口令
            "u@tcp(h:1)/d",
            {"user": "u", "password": "", "net": "tcp", "host": "h", "port": 1, "db": "d"},
        ),
        (
            # ★ unix socket —— 修复前它整个被误解析成 127.0.0.1:3306 + 空库名
            "u:p@unix(/var/run/mysqld.sock)/pandora_x",
            {"user": "u", "password": "p", "net": "unix", "host": "/var/run/mysqld.sock", "port": 0, "db": "pandora_x"},
        ),
    ],
)
def test_parses_known_shapes(dsn: str, want: dict) -> None:
    assert parse_go_dsn(dsn) == want


def test_query_params_are_stripped_from_db() -> None:
    assert parse_go_dsn("u:p@tcp(h:1)/d?parseTime=true&loc=UTC")["db"] == "d"


def test_password_may_contain_colon() -> None:
    """口令里带冒号是合法的 —— 别被 `user:pass` 的第一个冒号切错。"""
    assert parse_go_dsn("u:a:b:c@tcp(h:1)/d")["password"] == "a:b:c"


@pytest.mark.parametrize(
    "schema",
    ["physical-db", "physical`db", "physical/db", "a" * 65],
)
def test_parse_go_dsn_rejects_schema_that_cannot_be_safely_qualified(schema: str) -> None:
    """物理库名会进入全限定清理 SQL；非法标识符必须在启动解析阶段拒绝。"""
    with pytest.raises(ValueError, match="schema"):
        parse_go_dsn(f"u:p@tcp(h:1)/{schema}")


# ── 解析不了必须抛,不许回落 ────────────────────────────────────────

@pytest.mark.parametrize(
    "bad",
    [
        "",
        "not-a-dsn",
        "u:p@http(x)/d",          # 不认识的 net
        "u:p@tcp(h:1)",           # 缺 /db
        "justuser@tcp(h:1)",      # 同上
    ],
)
def test_unparseable_dsn_raises_instead_of_defaulting(bad: str) -> None:
    """★ 这是整个文件里最要紧的一条。

    回落默认值意味着:配置写错 → 服务**照常启动** → 连到 127.0.0.1:3306。
    在开发机上那里往往真的有一个 MySQL,于是"跑起来了",直到有人发现数据写到了
    另一个库里。抛异常会让它在启动闸就停住 —— 与 `redisx` 里"空端点不许静默连
    127.0.0.1"是同一条纪律。
    """
    with pytest.raises(ValueError):
        parse_go_dsn(bad)


def test_error_message_does_not_leak_password() -> None:
    """报错信息里不能带明文口令 —— 它会进日志。"""
    with pytest.raises(ValueError) as ei:
        parse_go_dsn("user:sup3rs3cret@nope(h)/d")
    assert "sup3rs3cret" not in str(ei.value)


# ── 分片身份哈希必须区分 net ────────────────────────────────────────

def test_shard_identity_distinguishes_network() -> None:
    """★ `shard_identity` 曾把 network 写死 `"tcp"`。

    身份哈希是分片拓扑的去重与防漂移判据(两个 DSN 撞同一 identity 会被拒批)。
    写死之后,同一份配置在 Go 与 Python 两栈会算出**不同的哈希** —— 拓扑代际
    校验在切换时无缘无故失败,而两边配置一个字节都没差。
    """
    tcp = shard_identity("u:p@tcp(h:3306)/d")
    unix_a = shard_identity("u:p@unix(/var/run/a.sock)/d")
    unix_b = shard_identity("u:p@unix(/var/run/b.sock)/d")
    assert len({tcp, unix_a, unix_b}) == 3, "三种拓扑必须是三个不同身份"


def test_shard_identity_is_stable_for_same_logical_db() -> None:
    """大小写与默认端口的等价形态必须落到同一个身份(否则去重形同虚设)。"""
    assert shard_identity("u:p@tcp(DB.Internal:3306)/d") == shard_identity(
        "u:p@tcp(db.internal)/d"
    )


# ── pool_kwargs 不许把解析结果整个 splat 给第三方 ────────────────────────

def test_pool_kwargs_does_not_leak_parser_fields_to_asyncmy() -> None:
    """★ `pool_kwargs` 只能输出 asyncmy 认识的键。

    真事:给 `parse_go_dsn` 加 `net` 字段(为了支持 unix socket)那次,
    `pool_kwargs` 里的 `**dsn` 把它原样漏进 `asyncmy.create_pool()` → TypeError,
    **19 个服务里凡是连库的全部起不来**,而报出来的事件是 `mysql_init_failed`,
    跟"DSN 解析"看不出任何关系。

    这条锁的是解耦本身:解析结果以后随便加字段,第三方参数只在 pool_kwargs
    翻译一次。
    """
    from pandorapy.config import MySQLConf
    from pandorapy.mysqlx import pool_kwargs

    # asyncmy.connect 接受的参数(只列我们会传的那些)
    allowed = {
        "user", "password", "db", "host", "port", "unix_socket",
        "minsize", "maxsize", "pool_recycle", "connect_timeout", "autocommit",
    }
    got = pool_kwargs(
        MySQLConf(), parse_go_dsn("u:p@tcp(h:3306)/d"), autocommit=True
    )
    assert set(got) <= allowed, f"漏了解析器内部字段给 asyncmy: {set(got) - allowed}"
    assert "net" not in got


@pytest.mark.parametrize("max_idle", [0, 1, 64])
def test_pool_kwargs_never_misrepresents_max_idle_as_prewarm(max_idle: int) -> None:
    """asyncmy 的 minsize 是启动预建数，不是 Go MaxIdleConns。"""
    from pandorapy.config import MySQLConf
    from pandorapy.mysqlx import pool_kwargs

    got = pool_kwargs(
        MySQLConf(max_open_conns=4, max_idle_conns=max_idle),
        parse_go_dsn("u:p@tcp(h:3306)/d"),
        autocommit=True,
    )
    assert got["minsize"] == 0
    assert got["maxsize"] == 4
    # 策划全量 14 个 MySQL pool：最坏 open=14*4=56，启动预热=14*0=0。
    assert 14 * got["maxsize"] == 56
    assert 14 * got["minsize"] == 0


def test_pool_kwargs_translates_unix_socket() -> None:
    """unix 档必须翻成 asyncmy 的 `unix_socket=`,而不是 host/port。"""
    from pandorapy.config import MySQLConf
    from pandorapy.mysqlx import pool_kwargs

    got = pool_kwargs(
        MySQLConf(), parse_go_dsn("u:p@unix(/var/run/mysqld.sock)/d"), autocommit=True
    )
    assert got["unix_socket"] == "/var/run/mysqld.sock"
    assert "host" not in got and "port" not in got


def test_pool_kwargs_rejects_partial_tls_profile_before_dial() -> None:
    """中心 TLS 两字段必须成对出现，不能把半配置静默降成明文。"""
    from pandorapy.config import MySQLConf
    from pandorapy.mysqlx import pool_kwargs

    with pytest.raises(ValueError, match=r"tls_ca_file.*tls_server_name"):
        pool_kwargs(
            MySQLConf(tls_ca_file=r"C:\missing\planner-ca.pem"),
            parse_go_dsn("u:p@tcp(db.intra:3306)/d"),
            autocommit=True,
        )


def test_pool_kwargs_rejects_tls_identity_different_from_dsn_host() -> None:
    """asyncmy 没有独立 server_hostname 参数，因此 endpoint host 必须就是证书身份。"""
    from pandorapy.config import MySQLConf
    from pandorapy.mysqlx import pool_kwargs

    with pytest.raises(ValueError, match=r"db-a\.intra.*db-b\.intra"):
        pool_kwargs(
            MySQLConf(
                tls_ca_file=r"C:\missing\planner-ca.pem",
                tls_server_name="db-b.intra",
            ),
            parse_go_dsn("u:p@tcp(db-a.intra:3306)/d"),
            autocommit=True,
        )


@pytest.mark.parametrize("param", ["tls=true", "tls=skip-verify", "ssl=True"])
def test_parse_go_dsn_rejects_boolean_or_driver_tls_params(param: str) -> None:
    """Python 只接受显式 SSLContext；DSN bool/driver mode 不能被静默忽略。"""
    with pytest.raises(ValueError, match=r"TLS|SSLContext"):
        parse_go_dsn(f"u:p@tcp(db.intra:3306)/d?{param}")


def _write_test_ca(tmp_path) -> str:  # noqa: ANN001
    import datetime as dt

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Pandora test CA")])
    now = dt.datetime.now(dt.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    path = tmp_path / "planner-ca.pem"
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    return str(path)


def _write_test_tls_material(tmp_path, certificate_ip: str) -> tuple[str, str, str]:  # noqa: ANN001
    import datetime as dt
    import ipaddress

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    now = dt.datetime.now(dt.UTC)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Pandora handshake CA")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(11)
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )
    server_key = ec.generate_private_key(ec.SECP256R1())
    server_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Pandora TLS server")])
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(server_name)
        .issuer_name(ca_name)
        .public_key(server_key.public_key())
        .serial_number(12)
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(hours=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address(certificate_ip))]),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    ca_path = tmp_path / "handshake-ca.pem"
    cert_path = tmp_path / "server.pem"
    key_path = tmp_path / "server-key.pem"
    ca_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    cert_path.write_bytes(server_cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        server_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return str(ca_path), str(cert_path), str(key_path)


def test_pool_kwargs_builds_verified_tls12_ssl_context(tmp_path) -> None:  # noqa: ANN001
    import ssl

    from pandorapy.config import MySQLConf
    from pandorapy.mysqlx import pool_kwargs

    got = pool_kwargs(
        MySQLConf(
            tls_ca_file=_write_test_ca(tmp_path),
            tls_server_name="db.intra",
        ),
        parse_go_dsn("u:p@tcp(db.intra:3306)/d"),
        autocommit=True,
    )
    context = got.get("ssl")
    assert isinstance(context, ssl.SSLContext), "只能向 asyncmy 传显式 SSLContext，不能传 bool"
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    assert context.minimum_version == ssl.TLSVersion.TLSv1_2
    assert len(context.get_ca_certs(binary_form=True)) == 1, (
        "中心 MySQL SSLContext 必须只装载 bundle CA，不能继承系统根"
    )


def test_pool_kwargs_rejects_same_host_certificate_trusted_only_by_system_store(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    import queue
    import socket
    import ssl
    import threading

    from pandorapy.config import MySQLConf
    from pandorapy.mysqlx import pool_kwargs

    bundle_dir = tmp_path / "bundle"
    system_dir = tmp_path / "system"
    bundle_dir.mkdir()
    system_dir.mkdir()
    bundle_ca, _, _ = _write_test_tls_material(bundle_dir, "127.0.0.1")
    system_ca, system_cert, system_key = _write_test_tls_material(system_dir, "127.0.0.1")
    # OpenSSL 的默认系统根 seam：旧实现 create_default_context() 会先读它，再追加 bundle。
    monkeypatch.setenv("SSL_CERT_FILE", system_ca)

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(3)
    host, port = listener.getsockname()
    server_result: queue.Queue[BaseException | None] = queue.Queue(maxsize=1)

    def serve_once() -> None:
        try:
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.minimum_version = ssl.TLSVersion.TLSv1_2
            server_context.load_cert_chain(system_cert, system_key)
            raw, _ = listener.accept()
            with raw, server_context.wrap_socket(raw, server_side=True):
                server_result.put(None)
        except BaseException as exc:  # 测试线程必须把失败交回主线程
            server_result.put(exc)

    thread = threading.Thread(target=serve_once, daemon=True)
    thread.start()
    try:
        got = pool_kwargs(
            MySQLConf(tls_ca_file=bundle_ca, tls_server_name=host),
            parse_go_dsn(f"u:p@tcp({host}:{port})/d"),
            autocommit=True,
        )
        assert len(got["ssl"].get_ca_certs(binary_form=True)) == 1
        with socket.create_connection((host, port), timeout=3) as raw:
            with pytest.raises(ssl.SSLCertVerificationError):
                got["ssl"].wrap_socket(raw, server_hostname=got["host"])
        thread.join(timeout=3)
        assert not thread.is_alive(), "本地 TLS 服务线程未按时退出"
        assert isinstance(server_result.get_nowait(), ssl.SSLError)
    finally:
        listener.close()


def test_pool_kwargs_ssl_context_completes_verified_local_handshake(tmp_path) -> None:  # noqa: ANN001
    import queue
    import socket
    import ssl
    import threading

    from pandorapy.config import MySQLConf
    from pandorapy.mysqlx import pool_kwargs

    ca_file, cert_file, key_file = _write_test_tls_material(tmp_path, "127.0.0.1")
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(3)
    host, port = listener.getsockname()
    server_result: queue.Queue[BaseException | None] = queue.Queue(maxsize=1)

    def serve_once() -> None:
        try:
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.minimum_version = ssl.TLSVersion.TLSv1_2
            server_context.maximum_version = ssl.TLSVersion.TLSv1_2
            server_context.load_cert_chain(cert_file, key_file)
            raw, _ = listener.accept()
            with raw, server_context.wrap_socket(raw, server_side=True) as secured:
                assert secured.recv(4) == b"ping"
                secured.sendall(b"pong")
            server_result.put(None)
        except BaseException as exc:  # 测试线程必须把失败交回主线程
            server_result.put(exc)

    thread = threading.Thread(target=serve_once, daemon=True)
    thread.start()
    try:
        got = pool_kwargs(
            MySQLConf(tls_ca_file=ca_file, tls_server_name=host),
            parse_go_dsn(f"u:p@tcp({host}:{port})/d"),
            autocommit=True,
        )
        with socket.create_connection((host, port), timeout=3) as raw:
            with got["ssl"].wrap_socket(raw, server_hostname=got["host"]) as secured:
                secured.sendall(b"ping")
                assert secured.recv(4) == b"pong"
        thread.join(timeout=3)
        assert not thread.is_alive(), "本地 TLS 服务线程未按时退出"
        assert server_result.get_nowait() is None
    finally:
        listener.close()


def test_pool_kwargs_ssl_context_rejects_trusted_wrong_identity(tmp_path) -> None:  # noqa: ANN001
    import queue
    import socket
    import ssl
    import threading

    from pandorapy.config import MySQLConf
    from pandorapy.mysqlx import pool_kwargs

    ca_file, cert_file, key_file = _write_test_tls_material(tmp_path, "127.0.0.2")
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(3)
    host, port = listener.getsockname()
    server_result: queue.Queue[BaseException | None] = queue.Queue(maxsize=1)

    def serve_once() -> None:
        try:
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.minimum_version = ssl.TLSVersion.TLSv1_2
            server_context.load_cert_chain(cert_file, key_file)
            raw, _ = listener.accept()
            with raw, server_context.wrap_socket(raw, server_side=True):
                server_result.put(None)
        except BaseException as exc:  # 测试线程必须把失败交回主线程
            server_result.put(exc)

    thread = threading.Thread(target=serve_once, daemon=True)
    thread.start()
    try:
        got = pool_kwargs(
            MySQLConf(tls_ca_file=ca_file, tls_server_name=host),
            parse_go_dsn(f"u:p@tcp({host}:{port})/d"),
            autocommit=True,
        )
        with socket.create_connection((host, port), timeout=3) as raw:
            with pytest.raises(ssl.SSLCertVerificationError):
                got["ssl"].wrap_socket(raw, server_hostname=got["host"])
        thread.join(timeout=3)
        assert not thread.is_alive(), "本地 TLS 服务线程未按时退出"
        assert isinstance(server_result.get_nowait(), ssl.SSLError)
    finally:
        listener.close()


@pytest.mark.parametrize("kind", ["missing", "malformed"])
def test_pool_kwargs_rejects_missing_or_malformed_ca(tmp_path, kind: str) -> None:  # noqa: ANN001
    from pandorapy.config import MySQLConf
    from pandorapy.mysqlx import pool_kwargs

    ca_file = tmp_path / "planner-ca.pem"
    if kind == "malformed":
        ca_file.write_text("not a PEM certificate\n", encoding="utf-8")
    with pytest.raises(ValueError, match="tls_ca_file"):
        pool_kwargs(
            MySQLConf(tls_ca_file=str(ca_file), tls_server_name="db.intra"),
            parse_go_dsn("u:p@tcp(db.intra:3306)/d"),
            autocommit=True,
        )


def test_pool_kwargs_without_tls_fields_keeps_local_plaintext_behavior() -> None:
    from pandorapy.config import MySQLConf
    from pandorapy.mysqlx import pool_kwargs

    got = pool_kwargs(
        MySQLConf(), parse_go_dsn("u:p@tcp(127.0.0.1:3306)/d"), autocommit=True
    )
    assert "ssl" not in got


def test_pool_kwargs_rejects_tls_over_unix_socket(tmp_path) -> None:  # noqa: ANN001
    from pandorapy.config import MySQLConf
    from pandorapy.mysqlx import pool_kwargs

    with pytest.raises(ValueError, match="requires tcp"):
        pool_kwargs(
            MySQLConf(
                tls_ca_file=_write_test_ca(tmp_path),
                tls_server_name="db.intra",
            ),
            parse_go_dsn("u:p@unix(/var/run/mysqld.sock)/d"),
            autocommit=True,
        )
