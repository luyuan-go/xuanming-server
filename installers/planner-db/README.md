# 策划中心 MySQL 信任 bundle

Git 只跟踪本说明。策划 SVN 的同一路径由发布维护者放入两个真实文件：

- `central-mysql.json`：非秘密的 provisioner HTTPS 地址和中心 MySQL endpoint；
- `planner-db-ca.pem`：只用于验证 provisioner 与 MySQL 服务端身份的内部 CA 证书。

`central-mysql.json` 固定格式：

```json
{
  "schema_version": 1,
  "provisioner_url": "https://pandora-planner-db.intra:7443/v1/enroll",
  "endpoint": {
    "host": "pandora-planner-db.intra",
    "port": 3306,
    "tls_server_name": "pandora-planner-db.intra"
  },
  "ca_file": "planner-db-ca.pem"
}
```

这里绝不放 enrollment token、MySQL 用户名/密码、admin/migration DSN、私钥或客户端证书。
CA 私钥只留在内部 PKI；SVN 仅分发公开 CA 证书。`endpoint.host` 必须出现在服务端证书
SAN 中，并与 `tls_server_name` 完全一致。

目录存在且两个文件都通过校验时，策划一键启动选择 `central-managed`，绝不回落本机 MySQL；
从未登记过中心 workspace 的机器缺少配置时继续使用既有 `local-owned`。一旦已有 central applied
state、runtime profile 或 workspace identity，bundle 丢失会 fail-closed，禁止断网/漏包后切到另一套
本机数据。中心模式首次双击只要求输入
管理员另行交付的 43 字符一次性 enrollment code；成功后稳定 `workspace_id` 写入当前用户
`%LOCALAPPDATA%\Pandora\planner-db\identity.json`，runtime 密码只以当前用户 DPAPI 密文保存。

发布维护者更新 endpoint/CA 后必须同时运行：

```powershell
pwsh -NoLogo -NoProfile -File tools/scripts/tests/mysql_runtime_profile_contract_test.ps1
pwsh -NoLogo -NoProfile -File tools/scripts/tests/planner_mysql_enrollment_contract_test.ps1
```

如果只轮换 runtime 凭据，必须递增服务端返回的 `credential.version`；profile fingerprint 会变化，
启动器据此整批刷新服务，禁止新旧凭据进程混跑。
