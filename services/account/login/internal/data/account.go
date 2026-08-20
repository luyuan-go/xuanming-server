// Package data 是 login 服务的"数据层"(repository)。
//
// W3 ②(2026-06-05)真实化:
//   - MySQL: pandora_account.accounts / account_devices / account_bans 三表
//   - Redis: pandora:sess:<player_id>      (hash, TTL 24h)        session 状态
//   - Redis: pandora:ticket:<jti>          (string, TTL 5min)     DSTicket 防重放(SETNX)
package data

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
)

// AccountIdentity 是账号名解析出来的账号身份(账号 / 角色分离,2026-08-18)。
//
// AccountID 与 PlayerID 现在是**两个层面**的东西,别再当同一个值用:
//   - AccountID 是账号身份,登录认证的产物,一个账号恒一个;
//   - PlayerID 在本结构里是 accounts 表的历史主键,语义已退化为「该账号的主角色指针」。
//     真正的「有哪些角色」权威是 account_roles 台账(见 account_role.go)。
//     expand 窗口内保留它,是因为旧 login 二进制仍按它工作;contract 阶段才删。
type AccountIdentity struct {
	// AccountID 0 = 存量行 account_id 列仍是 NULL(旧二进制注册),需要 BackfillAccountID 补铸。
	AccountID    uint64
	PlayerID     uint64
	PasswordHash string
}

// AccountRepo 是账号数据访问接口。biz 层依赖本接口,而不是具体实现,
// 方便在 mock / mysql 实现之间切换不动 biz/service。
type AccountRepo interface {
	// FindByAccount 根据账号名查账号身份 + bcrypt 哈希后的密码。
	// 找不到返回 ErrLoginAccountNotFound。
	FindByAccount(ctx context.Context, account string) (AccountIdentity, error)

	// FindByAccountID 按账号身份查账号行(两步登录的 EnterRole 用:封禁是账号级的,
	// 必须回查该账号的 accounts.player_id 才能查对封禁记录,不能拿要进入的角色 ID 去查)。
	// 找不到返回 ErrLoginAccountNotFound。
	FindByAccountID(ctx context.Context, accountID uint64) (AccountIdentity, error)

	// CreateAccount 新建账号(snowflake 分配的 accountID / playerID 传入)。
	// 账号已存在返回 ErrAlreadyExists。
	CreateAccount(ctx context.Context, accountID, playerID uint64, account, bcryptHash string) error

	// BackfillAccountID 给 account_id 仍为 NULL 的存量行补铸账号身份(滚动升级兼容窗)。
	//
	// 什么时候会为 NULL:000008 迁移已把**迁移那一刻**的所有账号回填完;之后若旧 login
	// 二进制(不认识 account_id 列)又注册了新账号,那一行就是 NULL。新 login 遇到它时
	// 就地补铸一个。
	//
	// 条件写 `WHERE player_id=? AND account_id IS NULL`:并发下只有一个请求能写成功,
	// 输的那个再读一次拿到赢家的值。返回**最终生效**的 account_id(不一定是传入的 candidate)。
	BackfillAccountID(ctx context.Context, playerID, candidate uint64) (uint64, error)

	// CheckBanned 检查账号 / 设备是否在有效封禁期内(account_bans 表 expires_at>now 或 NULL)。
	CheckBanned(ctx context.Context, playerID uint64, deviceID string) (banned bool, err error)

	// TouchDevice 记录最近一次登录设备(account_devices upsert)。失败由 biz 层只记日志。
	TouchDevice(ctx context.Context, playerID uint64, deviceID string) error

	// GetPlayerNo 读**当前角色**的角色编号(展示专用,player-no-and-login-surge.md §3;
	// 编号绑定角色实体而非账号,见 §3.6.1——今 player_id 即角色身份)。
	// 0 = 补号任务尚未分配(客户端显示「生成中」)。错误策略按入口分流:
	// Login 主路径必须 fail-soft(失败置 0 只记日志,绝不因展示字段拒登录——存量库尚未
	// 收敛到 pandora_account 000006 时靠该口径兜住);GetPlayerNo 补拉 RPC 必须把查询错误翻译成非 OK,
	// 不得伪装为 0。刻意不合并进 FindByAccount:列缺失不能把登录整链打挂。
	GetPlayerNo(ctx context.Context, playerID uint64) (uint64, error)
}

// =====================================================================
// MySQLAccountRepo:W3 ② 真实实现。
// =====================================================================

// MySQLAccountRepo 基于 *sql.DB 的账号仓储。
type MySQLAccountRepo struct {
	db *sql.DB
}

// NewMySQLAccountRepo 构造。db 由 pkg/mysqlx.MustNewClient 提供。
func NewMySQLAccountRepo(db *sql.DB) *MySQLAccountRepo {
	return &MySQLAccountRepo{db: db}
}

func (r *MySQLAccountRepo) FindByAccount(ctx context.Context, account string) (AccountIdentity, error) {
	const q = `SELECT player_id, account_id, password_hash FROM accounts WHERE account = ? LIMIT 1`
	var (
		id        AccountIdentity
		accountID sql.NullInt64
	)
	err := r.db.QueryRowContext(ctx, q, account).Scan(&id.PlayerID, &accountID, &id.PasswordHash)
	if errors.Is(err, sql.ErrNoRows) {
		return AccountIdentity{}, errcode.New(errcode.ErrLoginAccountNotFound, "account=%s not found", account)
	}
	if err != nil {
		return AccountIdentity{}, errcode.New(errcode.ErrInternal, "mysql find account: %v", err)
	}
	// NULL → 0,交给 biz 走 BackfillAccountID 补铸(旧二进制注册的存量行)。
	if accountID.Valid {
		id.AccountID = uint64(accountID.Int64)
	}
	return id, nil
}

func (r *MySQLAccountRepo) FindByAccountID(ctx context.Context, accountID uint64) (AccountIdentity, error) {
	if accountID == 0 {
		return AccountIdentity{}, errcode.New(errcode.ErrInvalidArg, "find account: accountID must be > 0")
	}
	// 走 uk_account_id 唯一索引点查。
	const q = `SELECT player_id, account_id, password_hash FROM accounts WHERE account_id = ? LIMIT 1`
	var (
		id     AccountIdentity
		acctID sql.NullInt64
	)
	err := r.db.QueryRowContext(ctx, q, accountID).Scan(&id.PlayerID, &acctID, &id.PasswordHash)
	if errors.Is(err, sql.ErrNoRows) {
		return AccountIdentity{}, errcode.New(errcode.ErrLoginAccountNotFound, "account_id=%d not found", accountID)
	}
	if err != nil {
		return AccountIdentity{}, errcode.New(errcode.ErrInternal, "mysql find account by id: %v", err)
	}
	if acctID.Valid {
		id.AccountID = uint64(acctID.Int64)
	}
	return id, nil
}

func (r *MySQLAccountRepo) CreateAccount(ctx context.Context, accountID, playerID uint64, account, bcryptHash string) error {
	const q = `INSERT INTO accounts(player_id, account_id, account, password_hash) VALUES (?, ?, ?, ?)`
	_, err := r.db.ExecContext(ctx, q, playerID, accountID, account, bcryptHash)
	if err != nil {
		// 1062 = ER_DUP_ENTRY,字符串匹配避免强依赖 mysql driver 错误类型
		if isDupErr(err) {
			return errcode.New(errcode.ErrAlreadyExists, "account=%s already exists", account)
		}
		return errcode.New(errcode.ErrInternal, "mysql create account: %v", err)
	}
	return nil
}

func (r *MySQLAccountRepo) BackfillAccountID(ctx context.Context, playerID, candidate uint64) (uint64, error) {
	if playerID == 0 || candidate == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg,
			"backfill account_id: playerID/candidate must be > 0 (got %d/%d)", playerID, candidate)
	}
	// 条件写:account_id IS NULL 才写。并发只有一个赢家,输的那个 RowsAffected=0。
	const upd = `UPDATE accounts SET account_id = ? WHERE player_id = ? AND account_id IS NULL`
	res, err := r.db.ExecContext(ctx, upd, candidate, playerID)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "mysql backfill account_id: %v", err)
	}
	if n, aerr := res.RowsAffected(); aerr == nil && n > 0 {
		return candidate, nil
	}
	// 没写成:要么并发被别人抢先,要么这一行根本不存在。回读拿**最终生效**的值 ——
	// 绝不返回 candidate 假装成功,否则两个并发请求会各自以为自己的 account_id 生效,
	// 后续按不同 account_id 建角色台账,同一个账号裂成两个。
	const sel = `SELECT account_id FROM accounts WHERE player_id = ? LIMIT 1`
	var current sql.NullInt64
	if serr := r.db.QueryRowContext(ctx, sel, playerID).Scan(&current); serr != nil {
		if errors.Is(serr, sql.ErrNoRows) {
			return 0, errcode.New(errcode.ErrLoginAccountNotFound, "account player_id=%d not found", playerID)
		}
		return 0, errcode.New(errcode.ErrInternal, "mysql read back account_id: %v", serr)
	}
	if !current.Valid || current.Int64 <= 0 {
		// 更新说"没改到"、回读又说"还是 NULL":两次读到互相矛盾的状态,不能猜。
		return 0, errcode.New(errcode.ErrInternal,
			"backfill account_id inconsistent for player_id=%d (update no-op but column still NULL)", playerID)
	}
	return uint64(current.Int64), nil
}

func (r *MySQLAccountRepo) CheckBanned(ctx context.Context, playerID uint64, deviceID string) (bool, error) {
	const q = `SELECT COUNT(*) FROM account_bans
WHERE (expires_at IS NULL OR expires_at > UTC_TIMESTAMP())
  AND ((player_id IS NOT NULL AND player_id = ?) OR (device_id IS NOT NULL AND device_id = ?))`
	var cnt int
	if err := r.db.QueryRowContext(ctx, q, playerID, deviceID).Scan(&cnt); err != nil {
		return false, errcode.New(errcode.ErrInternal, "mysql check banned: %v", err)
	}
	return cnt > 0, nil
}

// GetPlayerNo PK 点查角色编号(登录路径 +1 次毫秒级往返,不进 5s 预算的服务扇出账,
// 压测审核【必修-1】口径不受影响)。NULL / 行不存在均返回 0(未分配)。
func (r *MySQLAccountRepo) GetPlayerNo(ctx context.Context, playerID uint64) (uint64, error) {
	var playerNo, legacyNo sql.NullInt64
	err := r.db.QueryRowContext(ctx,
		`SELECT player_no, register_no FROM accounts WHERE player_id = ? LIMIT 1`, playerID).Scan(&playerNo, &legacyNo)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "mysql get player_no: %v", err)
	}
	if playerNo.Valid && legacyNo.Valid && playerNo.Int64 != legacyNo.Int64 {
		return 0, errcode.New(errcode.ErrInternal,
			"mysql get player_no: player/register 双列冲突 player_id=%d player_no=%d register_no=%d",
			playerID, playerNo.Int64, legacyNo.Int64)
	}
	if playerNo.Valid {
		return uint64(playerNo.Int64), nil
	}
	if legacyNo.Valid {
		return uint64(legacyNo.Int64), nil
	}
	if !playerNo.Valid {
		return 0, nil
	}
	return uint64(playerNo.Int64), nil
}

// ResolvePlayerNos 用一次 IN 查询批量读取角色展示编号，供内部 team 快照投影使用。
// 调用方已经排序、去重并限制批量大小；本层仍拒绝空输入，避免生成无效 SQL。
// 返回 map 的 key 只使用 player_id；player_no 永远只是展示值。
func (r *MySQLAccountRepo) ResolvePlayerNos(ctx context.Context, playerIDs []uint64) (map[uint64]uint64, error) {
	if len(playerIDs) == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_ids required")
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(playerIDs)), ",")
	args := make([]any, len(playerIDs))
	for i, playerID := range playerIDs {
		if playerID == 0 {
			return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
		}
		args[i] = playerID
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT player_id, player_no, register_no FROM accounts WHERE player_id IN (`+placeholders+`)`, args...)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "mysql resolve player_nos: %v", err)
	}
	defer rows.Close()

	result := make(map[uint64]uint64, len(playerIDs))
	for rows.Next() {
		var playerID uint64
		var playerNo, legacyNo sql.NullInt64
		if err := rows.Scan(&playerID, &playerNo, &legacyNo); err != nil {
			return nil, errcode.New(errcode.ErrInternal, "mysql resolve player_nos scan: %v", err)
		}
		if playerNo.Valid && legacyNo.Valid && playerNo.Int64 != legacyNo.Int64 {
			return nil, errcode.New(errcode.ErrInternal,
				"mysql resolve player_nos: player/register 双列冲突 player_id=%d", playerID)
		}
		switch {
		case playerNo.Valid:
			result[playerID] = uint64(playerNo.Int64)
		case legacyNo.Valid:
			result[playerID] = uint64(legacyNo.Int64)
		default:
			result[playerID] = 0
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errcode.New(errcode.ErrInternal, "mysql resolve player_nos rows: %v", err)
	}
	return result, nil
}

func (r *MySQLAccountRepo) TouchDevice(ctx context.Context, playerID uint64, deviceID string) error {
	if deviceID == "" {
		return nil
	}
	const q = `INSERT INTO account_devices(player_id, device_id, last_login_at)
VALUES (?, ?, UTC_TIMESTAMP())
ON DUPLICATE KEY UPDATE last_login_at = UTC_TIMESTAMP()`
	if _, err := r.db.ExecContext(ctx, q, playerID, deviceID); err != nil {
		return errcode.New(errcode.ErrInternal, "mysql touch device: %v", err)
	}
	return nil
}

// isDupErr 粗略判断 MySQL 唯一键冲突,不依赖 mysql driver 强类型。
func isDupErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "1062") || strings.Contains(s, "Duplicate entry")
}

// =====================================================================
// SessionRepo:Redis 上的玩家 session 状态。
// =====================================================================

// SessionRepo 维护 pandora:sess:<player_id> hash + TTL。
//
// hash 字段:
//
//	token      string  当前签的 session JWT(全文,debug 用)
//	jti        string  session JWT 的 jti(便于将来 jti 黑名单)
//	device_id  string  当前设备
//	exp_ms     int64   session 过期 unix ms
//	gen        uint64  MySQL 分配的登录单调代际(R7 收口;0/缺失 = 无定序权威旧写入)
type SessionRepo interface {
	// Set 写入/覆盖玩家会话。gen>0 时为条件写:仅当现存 gen 缺失或小于本次 gen 才
	// 覆盖,否则返回 ErrSessionSuperseded(并发登录中已有更新一代完成写入,本次
	// 登录必须失败,不得交付凭据)。gen==0 = 无 MySQL 定序权威(dev 裸跑),保持
	// 历史无条件覆盖语义。
	Set(ctx context.Context, playerID uint64, token, jti, deviceID string, ttl time.Duration, gen uint64) error
	Delete(ctx context.Context, playerID uint64) error
	// GetJTI 读当前 session 的 jti(P0 修复 2026-07-15,session 现行性门)。
	// found=false = 无 session(已登出/过期/从未登录)。
	GetJTI(ctx context.Context, playerID uint64) (jti string, found bool, err error)
	// DeleteIfJTI 仅当当前 session 的 jti 匹配时才删除(CAS)。
	// 防止旧设备的迟到 Logout 误删新登录的 session(顶号后新设备被踢)。
	DeleteIfJTI(ctx context.Context, playerID uint64, jti string) (deleted bool, err error)
	// FenceFailedSet 收口一次结果不确定的 Set(gen>0)：若 Redis 当前 generation
	// 不高于失败代际，就清除全部会话能力并把 gen 推到失败代际；更高代际已写入时
	// no-op，绝不影响赢家。不能恢复“即时前代”——它可能也是从未交付的候选会话。
	// ttl 给无 key/短 TTL 墓碑提供有界 fencing 窗口，防迟到低代际写复活。
	FenceFailedSet(ctx context.Context, playerID uint64, jti string, gen uint64, ttl time.Duration) (fenced bool, err error)
}

// RedisSessionRepo 基于 go-redis/v9 的 SessionRepo 实现。
type RedisSessionRepo struct {
	rdb redis.UniversalClient
}

// NewRedisSessionRepo 构造。
func NewRedisSessionRepo(rdb redis.UniversalClient) *RedisSessionRepo {
	return &RedisSessionRepo{rdb: rdb}
}

func sessKey(playerID uint64) string {
	return fmt.Sprintf("pandora:sess:%d", playerID)
}

// setIfNewerGenScript:单 key 原子「代际比较 + 覆盖」(R7/R13 收口)。
// 现存 gen 缺失/更小 → 覆盖并刷 TTL,返回 1;否则零写入返回 0(本次登录已被更新
// 一代顶掉)。结果不确定时绝不恢复即时前代：A→B→C 交错中即时前代 B 可能从未
// 交付。补偿统一走 FenceFailedSet，清能力、保留单调水位后由可重试 Login 自愈。
var setIfNewerGenScript = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], 'gen')
if cur then
	local cur_gen = tonumber(cur)
	local next_gen = tonumber(ARGV[5])
	if cur_gen > next_gen then
		return 0
	end
	if cur_gen == next_gen then
		-- go-redis 默认会重试网络结果不确定的命令。第一次 EVAL 已落地、仅回包
		-- 丢失时，重试必须把同一 (jti,gen) 识别为幂等成功；否则会把自己的
		-- 已确认写误报成被并发登录顶掉。相同 generation 却不同 jti 不可能由
		-- 正常 MySQL 分配产生，单独返回完整性冲突，不能冒充幂等。
		if redis.call('HGET', KEYS[1], 'jti') == ARGV[2] then
			return 2
		end
		return -1
	end
end
redis.call('HSET', KEYS[1],
	'token', ARGV[1], 'jti', ARGV[2], 'device_id', ARGV[3], 'exp_ms', ARGV[4], 'gen', ARGV[5])
redis.call('HDEL', KEYS[1], '_rollback_token', '_rollback_jti',
	'_rollback_device_id', '_rollback_exp_ms')
redis.call('PEXPIRE', KEYS[1], ARGV[6])
return 1
`)

func (r *RedisSessionRepo) Set(ctx context.Context, playerID uint64, token, jti, deviceID string, ttl time.Duration, gen uint64) error {
	if ttl < time.Millisecond {
		return errcode.New(errcode.ErrInvalidArg,
			"invalid session ttl for player %d: %s (minimum 1ms)", playerID, ttl)
	}
	key := sessKey(playerID)
	expMs := time.Now().Add(ttl).UnixMilli()
	if gen > 0 {
		n, err := setIfNewerGenScript.Run(ctx, r.rdb, []string{key},
			token, jti, deviceID, expMs, gen, ttl.Milliseconds()).Int64()
		if err != nil {
			return errcode.New(errcode.ErrUnavailable, "redis sess set unavailable: %v", err)
		}
		if n == 0 {
			return errcode.New(errcode.ErrSessionSuperseded,
				"login superseded by a newer concurrent login (player %d gen %d)", playerID, gen)
		}
		if n < 0 {
			return errcode.New(errcode.ErrUnavailable,
				"session generation conflict for player %d gen %d; retry login", playerID, gen)
		}
		return nil
	}
	// gen==0:dev 裸跑无 MySQL 定序权威,保持历史无条件覆盖;同时清掉可能残留的
	// gen 字段,避免陈旧代际把后续 dev 登录当成"被顶"拒绝。
	pipe := r.rdb.TxPipeline()
	pipe.HSet(ctx, key,
		"token", token,
		"jti", jti,
		"device_id", deviceID,
		"exp_ms", expMs,
	)
	pipe.HDel(ctx, key, "gen")
	pipe.HDel(ctx, key, "_rollback_token", "_rollback_jti", "_rollback_device_id", "_rollback_exp_ms")
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return errcode.New(errcode.ErrUnavailable, "redis sess set unavailable: %v", err)
	}
	return nil
}

func (r *RedisSessionRepo) Delete(ctx context.Context, playerID uint64) error {
	if err := r.rdb.Del(ctx, sessKey(playerID)).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return errcode.New(errcode.ErrInternal, "redis sess del: %v", err)
	}
	return nil
}

func (r *RedisSessionRepo) GetJTI(ctx context.Context, playerID uint64) (string, bool, error) {
	jti, err := r.rdb.HGet(ctx, sessKey(playerID), "jti").Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, errcode.New(errcode.ErrInternal, "redis sess get jti: %v", err)
	}
	return jti, jti != "", nil
}

// deleteIfJTIScript:hash 字段 jti 与参数相等才 DEL(原子 CAS,防迟到 Logout 误删新 session)。
var deleteIfJTIScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "jti") == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

func (r *RedisSessionRepo) DeleteIfJTI(ctx context.Context, playerID uint64, jti string) (bool, error) {
	n, err := deleteIfJTIScript.Run(ctx, r.rdb, []string{sessKey(playerID)}, jti).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, errcode.New(errcode.ErrInternal, "redis sess del-if-jti: %v", err)
	}
	return n > 0, nil
}

// fenceFailedSetScript 对结果不确定的失败代际做单调无能力墓碑：
//   - Redis 仍是失败代际，或还是更老的已交付/未交付代际 → 清能力并推进到失败 gen；
//   - 更新代际已经写入 → no-op，绝不撤销赢家。
//
// 这里故意按 generation <= failedGen fencing，而不只匹配 failedJTI：C 写入结果不确定
// 且实际未落 Redis 时，Redis 可能仍停在同样未交付的 B；只匹配 C 会把 B 永久留成
// current。墓碑 TTL 使用本次 session TTL，保证迟到的低代际命令不能在 key 过早过期后
// 复活。ARGV[1] 保留 failedJTI 作为脚本调用契约/审计参数，不参与“恢复谁”的猜测。
var fenceFailedSetScript = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], 'gen')
if cur and tonumber(cur) > tonumber(ARGV[2]) then
	return 0
end
redis.call('HDEL', KEYS[1], 'token', 'jti', 'device_id', 'exp_ms',
	'_rollback_token', '_rollback_jti', '_rollback_device_id', '_rollback_exp_ms')
redis.call('HSET', KEYS[1], 'gen', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`)

func (r *RedisSessionRepo) FenceFailedSet(ctx context.Context, playerID uint64, jti string, gen uint64, ttl time.Duration) (bool, error) {
	if jti == "" || gen == 0 || ttl < time.Millisecond {
		return false, errcode.New(errcode.ErrInvalidArg,
			"invalid failed-session fence: player %d jti_present=%t gen %d ttl %s",
			playerID, jti != "", gen, ttl)
	}
	n, err := fenceFailedSetScript.Run(ctx, r.rdb, []string{sessKey(playerID)}, jti, gen, ttl.Milliseconds()).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, errcode.New(errcode.ErrUnavailable, "redis sess fence-failed-set unavailable: %v", err)
	}
	return n > 0, nil
}

// =====================================================================
// TicketJTIRepo:DSTicket 防重放(Verify 时 SETNX)。
// =====================================================================

// TicketJTIRepo 维护 pandora:ticket:<jti> 短期标记。
//
// 语义:首次 Verify 时 SETNX 成功 → 票据可用;再次 SETNX 失败 → ErrLoginTicketReplayed。
type TicketJTIRepo interface {
	MarkUsed(ctx context.Context, jti string, ttl time.Duration) error
}

// AdmissionMarkerStatus 区分 marker 不存在、首次创建、同 attempt 已存在与冲突。
type AdmissionMarkerStatus uint8

const (
	AdmissionMarkerMissing AdmissionMarkerStatus = iota
	AdmissionMarkerCreated
	AdmissionMarkerExisting
	AdmissionMarkerConflict
)

// AdmissionTicketJTIRepo 是 Redis authority 在线准入的幂等消费扩展。
// attemptOwner 跨同实例普通 token 轮换稳定；acceptedCredentialHash 永久记录首次接受
// 的完整 active tuple。Peek 与原子 Mark 分开，使 TicketUsecase 能按 missing/existing
// 选择严格首次绑定或稳定身份重认，且绝不先覆盖已有 owner。
type AdmissionTicketJTIRepo interface {
	PeekAdmission(ctx context.Context, jti, attemptOwner string) (AdmissionMarkerStatus, error)
	MarkUsedByAdmission(ctx context.Context, jti, attemptOwner, acceptedCredentialHash string, ttl time.Duration) (AdmissionMarkerStatus, error)
}

// RedisTicketJTIRepo 基于 go-redis/v9 的 TicketJTIRepo 实现。
type RedisTicketJTIRepo struct {
	rdb                   redis.UniversalClient
	admissionReplayWindow time.Duration
}

// NewRedisTicketJTIRepo 构造。
func NewRedisTicketJTIRepo(rdb redis.UniversalClient) *RedisTicketJTIRepo {
	return &RedisTicketJTIRepo{rdb: rdb, admissionReplayWindow: 30 * time.Second}
}

func ticketKey(jti string) string {
	return fmt.Sprintf("pandora:ticket:%s", jti)
}

func (r *RedisTicketJTIRepo) MarkUsed(ctx context.Context, jti string, ttl time.Duration) error {
	if jti == "" {
		return errcode.New(errcode.ErrInvalidArg, "empty jti")
	}
	ok, err := r.rdb.SetNX(ctx, ticketKey(jti), 1, ttl).Result()
	if err != nil {
		return errcode.New(errcode.ErrInternal, "redis ticket setnx: %v", err)
	}
	if !ok {
		return errcode.New(errcode.ErrLoginTicketReplayed, "ticket jti=%s already used", jti)
	}
	return nil
}

var peekTicketAdmissionScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local version, attempt, credential, accepted_at, replay_until = string.match(
  current, '^([^|]+)|([^|]+)|([^|]+)|([^|]+)|([^|]+)$')
if version ~= 'admission-v4' or string.len(attempt or '') ~= 64 or not string.match(attempt, '^[0-9a-f]+$') or
   string.len(credential or '') ~= 64 or not string.match(credential, '^[0-9a-f]+$') or
   not tonumber(accepted_at) or not tonumber(replay_until) or tonumber(replay_until) < tonumber(accepted_at) then
  return 3
end
local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
if attempt == ARGV[1] and now_ms <= tonumber(replay_until) then
  return 2
end
return 3
`)

var markTicketAdmissionScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
if not current then
  local replay_until = now_ms + tonumber(ARGV[4])
  local marker = 'admission-v4|' .. ARGV[1] .. '|' .. ARGV[2] .. '|' .. now_ms .. '|' .. replay_until
  redis.call('PSETEX', KEYS[1], ARGV[3], marker)
  return 1
end
local version, attempt, credential, accepted_at, replay_until = string.match(
  current, '^([^|]+)|([^|]+)|([^|]+)|([^|]+)|([^|]+)$')
if version == 'admission-v4' and string.len(attempt or '') == 64 and string.match(attempt, '^[0-9a-f]+$') and
   string.len(credential or '') == 64 and string.match(credential, '^[0-9a-f]+$') and
   tonumber(accepted_at) and tonumber(replay_until) and tonumber(replay_until) >= tonumber(accepted_at) and
   attempt == ARGV[1] and now_ms <= tonumber(replay_until) then
  return 2
end
return 0
`)

const admissionMarkerVersion = "admission-v4"

func validAdmissionDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// PeekAdmission 只读现有 marker。legacy value="1"、坏格式与不同 attempt 一律返回
// Conflict（安全 replay），Redis 故障返回 Unavailable。
func (r *RedisTicketJTIRepo) PeekAdmission(ctx context.Context, jti, attemptOwner string) (AdmissionMarkerStatus, error) {
	if jti == "" || !validAdmissionDigest(attemptOwner) {
		return AdmissionMarkerConflict, errcode.New(errcode.ErrInvalidArg, "invalid admission marker lookup")
	}
	if r == nil || r.rdb == nil {
		return AdmissionMarkerConflict, errcode.New(errcode.ErrUnavailable, "ticket admission replay authority unavailable")
	}
	result, err := peekTicketAdmissionScript.Run(ctx, r.rdb, []string{ticketKey(jti)}, attemptOwner).Int64()
	if err != nil {
		return AdmissionMarkerConflict, errcode.NewCause(errcode.ErrUnavailable, err, "redis ticket admission lookup failed")
	}
	switch result {
	case 0:
		return AdmissionMarkerMissing, nil
	case 2:
		return AdmissionMarkerExisting, nil
	default:
		return AdmissionMarkerConflict, nil
	}
}

// MarkUsedByAdmission 在单条 Redis Lua 中完成 absent→versioned marker；同 attempt_owner
// 只确认已存在，绝不覆盖首次 accepted_credential_hash。不同 owner、legacy "1"、坏格式均冲突。
func (r *RedisTicketJTIRepo) MarkUsedByAdmission(
	ctx context.Context,
	jti, attemptOwner, acceptedCredentialHash string,
	ttl time.Duration,
) (AdmissionMarkerStatus, error) {
	if jti == "" || ttl <= 0 || !validAdmissionDigest(attemptOwner) || !validAdmissionDigest(acceptedCredentialHash) {
		return AdmissionMarkerConflict, errcode.New(errcode.ErrInvalidArg, "invalid admission ticket marker")
	}
	if r == nil || r.rdb == nil {
		return AdmissionMarkerConflict, errcode.New(errcode.ErrUnavailable, "ticket admission replay authority unavailable")
	}
	replayWindow := r.admissionReplayWindow
	if replayWindow <= 0 {
		replayWindow = 30 * time.Second
	}
	result, err := markTicketAdmissionScript.Run(ctx, r.rdb, []string{ticketKey(jti)},
		attemptOwner, acceptedCredentialHash, ttl.Milliseconds(), replayWindow.Milliseconds()).Int64()
	if err != nil {
		return AdmissionMarkerConflict, errcode.NewCause(errcode.ErrUnavailable, err, "redis ticket admission mark failed")
	}
	switch result {
	case 1:
		return AdmissionMarkerCreated, nil
	case 2:
		return AdmissionMarkerExisting, nil
	default:
		return AdmissionMarkerConflict, errcode.New(errcode.ErrLoginTicketReplayed, "ticket jti=%s already used by another admission", jti)
	}
}

// SweepStaleDevices 处理 last_login_at 超保留期的设备绑定行(保留期清理,§9.24)。
// **mode 默认 ModeReportOnly:只统计待清理量并 WARN 告警,一行都不删**(用户指令)。
// account_devices 的 device_id 由客户端上报,单账号可无限堆新设备行 → 按最近登录时间兜底
// 有界:被删设备下次登录时 TouchDevice upsert 自然重建,只丢历史最近登录记录,无业务语义。
// 多副本并发调用安全(各删各的行)。走 idx_last_login 索引。
func SweepStaleDevices(ctx context.Context, db *sql.DB, mode dbguard.Mode, retentionDays, limit int) (dbguard.Outcome, error) {
	out, err := dbguard.SweepTable(ctx, db, mode, "pandora_account", "account_devices",
		"last_login_at < DATE_SUB(NOW(), INTERVAL ? DAY)", limit, retentionDays)
	if err != nil {
		return out, errcode.New(errcode.ErrInternal, "sweep stale devices: %v", err)
	}
	return out, nil
}
