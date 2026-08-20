"""player 数据层(MySQL)—— 对应 Go 侧 services/account/player/internal/data/*.go。

库表(deploy/mysql-init/04-player-tables.sql + tools/migrate pandora_player):

    players               玩家档案(PK player_id,uk nickname)
    player_mmr            分池段位(PK player_id+rating_pool)
    player_heroes         英雄解锁(uk player_id+hero_id)
    mmr_history           MMR 变化历史 + 幂等键(uk player_id+idempotency_key)
    player_attributes     属性加点
    player_equipment      出战装备预设(uk_player_slot / uk_player_instance)
    player_talents        天赋分配(带 spent_points)
    player_skill_cards    技能卡持有
    player_skill_slots    卡槽装配(uk_player_slot / uk_player_card_once)
    attr_point_grants     加点授予幂等收据
    talent_point_grants   天赋点授予幂等收据
    skill_card_grants     发卡幂等收据
    exp_history           经验入账幂等收据(uk player_id+idempotency_key)
    player_push_outbox    经验推送事务出箱
    player_reward_claims  领奖位图(LONGBLOB + version 乐观锁)

★ 三条纪律逐条照抄 Go,写错都不报错:

  ① **1062 语义**:唯一键冲突 = 幂等命中,读回已记录的权威值返回,绝不重复入账。
     漏了它,重投的同一场对局会被二次加分。
  ② **锁序**固定 players → player_mmr/player_attributes/... → *_history,全仓无反向
     路径,不成环。ApplyMMRChange 刻意锁 players 而不是 player_mmr:该池首战时
     player_mmr **一行都没有**,而 TiDB 没有 gap 锁 —— 零行上的 FOR UPDATE 一把锁都
     不加,两局并发都会判自己是首战、各自 INSERT 覆盖。
  ③ **配置相关的判定(每级消耗 / 等级上限 / 卡是否存在 / 槽位数)全部由 biz 按配置表
     算好后传入**,repo 层看不到配置表。这样"改配置表"永远不需要动 SQL,也不会出现
     repo 按 Σ 等级 反推而在 cost_per_level≠1 时算少扣的情况。

★ autocommit=True(与 Go `database/sql` 默认语义一致),需要原子性的地方显式
  `conn.begin()` 包起来 —— 与 mail 同一档,理由见 mysqlx.pool_kwargs 的注释。
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as _dt
import time

from pandora.player.v1 import player_pb2 as ppb

from pandorapy import dbguard
from pandorapy import errcode
from pandorapy import mysqlx
from pandorapy import rating as prating
from pandorapy.services.player import models as m
from pandorapy.services.player.experience import advance_experience

# 库名。dbguard.sweep_table 需要显式 schema(它把表名拼进 SQL,不走参数绑定)。
DB_SCHEMA = "pandora_player"

# INT 列(player_attributes.points / players.unspent_attr_points)的有符号上界。
MAX_INT32 = 2**31 - 1

# 「该行实际花掉多少天赋点」的 SQL 口径,读取侧统一用它。
#
# spent_points 为 0 时回退到 level,是 §9.21 滚动升级共存窗口的桥:老副本的 INSERT
# 不带 spent_points,新列取默认 0,新副本读到 0 会把这份分配当"没花点"(可点数虚高)。
# 回退到 level 等同当前线上口径(全部节点 cost_per_level=1);等所有副本换新后所有行
# 都会带上真实消耗,该分支自然不再命中。cost_per_level≥1 且 level≥1,真实消耗恒 >0,
# 所以 0 只可能来自老副本写入,不会误判正常行。
TALENT_SPENT_EXPR = "IF(spent_points > 0, spent_points, level)"


def _internal(msg: str, exc: BaseException) -> errcode.PandoraError:
    return errcode.PandoraError(errcode.ErrInternal, "%s: %s", msg, exc)


class MySQLPlayerRepo:
    """基于 asyncmy 连接池的 player 仓储。

    ★ `schema` 是**与 Go 的一处有据差异**:Go 侧 sweepByCreatedAt 把 "pandora_player"
      写死进 dbguard.SweepTable(表名不能参数化,只能拼进 SQL)。写死的失败模式是——
      DSN 指向另一个库名时,清理会去扫**另一个库**的同名表:report_only 下报出别人的
      待清理量,delete 下删别人的行,两种都不报错。这里改成从 DSN 取到的实际库名注入,
      默认值仍是 DB_SCHEMA,标准部署行为逐字不变。
    """

    __slots__ = ("_pool", "_schema")

    def __init__(self, pool, schema: str = DB_SCHEMA) -> None:  # noqa: ANN001 —— asyncmy Pool
        self._pool = pool
        self._schema = schema or DB_SCHEMA

    # ── 事务小工具 ────────────────────────────────────────────────────────

    @contextlib.asynccontextmanager
    async def _tx(self, what: str):
        """开一个事务并交出 cursor;正常退出 commit,异常回滚后原样抛。

        ★ `asyncio.CancelledError` **必须先放行再回滚**:grpc.aio 用取消终止在途
        handler,把它当普通异常吞成业务错误会让优雅停机时客户端收到一批莫名其妙的
        业务失败,而且取消没有穿透 → 排空并没有按设计发生(§9.16)。
        """
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    yield cur
                await conn.commit()
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise

    @contextlib.asynccontextmanager
    async def _cursor(self):
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            yield cur

    # ── 档案 ──────────────────────────────────────────────────────────────

    async def ensure_profile(self, player_id: int, default_nickname: str, base_mmr: int) -> bool:
        """懒建档。INSERT IGNORE 语义:行已存在则**完全不动**(昵称不覆盖)。

        返回 created —— 本次是否真的建了档。login 播种角色名靠它判断"名字有没有真的
        落下去",不能靠"没报错"推断:建档与"早就存在"两种情况都不报错。

        ⚠️ 昵称冲突(uk_nickname 被别的玩家占了)在 INSERT IGNORE 下**不会报错**,
        而是静默不插入 → created=False 且该玩家至此仍无档案。调用方必须按 created=False
        复查,不能默认"没建就是已经有了"。
        """
        # expand 期仍写 players.mmr:旧副本以它作为 default 池的兼容权威。
        sql = (
            "INSERT IGNORE INTO players "
            "(player_id, nickname, level, mmr, avatar, total_battles, total_wins) "
            "VALUES (%s, %s, 1, %s, '', 0, 0)"
        )
        try:
            async with self._cursor() as cur:
                await cur.execute(sql, (player_id, default_nickname, base_mmr))
                return (cur.rowcount or 0) > 0
        except asyncio.CancelledError:
            raise
        except errcode.PandoraError:
            raise
        except BaseException as exc:
            raise _internal(f"ensure profile player={player_id}", exc) from exc

    async def list_nicknames(self, player_ids: list[int]) -> dict[int, str]:
        """批量反查角色显示名(Hub DS 铭牌用)。

        只返回**查到的**行:请求里有、结果里没有 = 该角色无档案。调用方据此区分
        「查不到」与「名字是空串」—— 用零值占位会让 DS 把一个真名字覆盖成空。

        刻意不在这里建档:本方法是纯只读旁路,给它加写副作用会让「看一眼名字」变成
        能给任意 player_id 造档案的入口。
        """
        if not player_ids:
            return {}
        placeholders = ",".join(["%s"] * len(player_ids))
        sql = f"SELECT player_id, nickname FROM players WHERE player_id IN ({placeholders})"  # noqa: S608
        try:
            async with self._cursor() as cur:
                await cur.execute(sql, tuple(player_ids))
                rows = await cur.fetchall()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal("list nicknames", exc) from exc
        return {int(r[0]): str(r[1]) for r in rows or ()}

    async def get_profile(self, player_id: int) -> ppb.PlayerProfile | None:
        """只读 players 表(账号级档案)。not found → None。

        **分池段位不在这里** —— 它按 rating_pool 存 player_mmr;players.mmr 仅是滚动
        升级的 default 投影。biz 另调 list_ratings 组装进 PlayerProfile.ratings。
        刻意不 JOIN:段位是一对多,JOIN 会让单行扫描变成需要去重的多行结果,而档案本身
        (昵称/等级/战绩)与打了几个池无关。
        """
        sql = (
            "SELECT nickname, level, mmr, avatar, "
            "UNIX_TIMESTAMP(created_at)*1000, UNIX_TIMESTAMP(last_seen_at)*1000, "
            "total_battles, total_wins, exp "
            "FROM players WHERE player_id = %s LIMIT 1"
        )
        try:
            async with self._cursor() as cur:
                await cur.execute(sql, (player_id,))
                row = await cur.fetchone()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"query profile player={player_id}", exc) from exc
        if row is None:
            return None
        return ppb.PlayerProfile(
            player_id=player_id,
            nickname=str(row[0]),
            level=int(row[1]),
            mmr=int(row[2]),
            avatar=str(row[3]),
            created_at_ms=int(row[4] or 0),
            last_seen_ms=int(row[5] or 0),
            total_battles=int(row[6]),
            total_wins=int(row[7]),
            exp_in_level=int(row[8]),
        )

    async def update_nickname(self, player_id: int, nickname: str) -> None:
        """改昵称。被占用 → ErrPlayerNicknameTaken;玩家不存在 → ErrPlayerNotFound。"""
        try:
            async with self._cursor() as cur:
                try:
                    await cur.execute(
                        "UPDATE players SET nickname = %s WHERE player_id = %s",
                        (nickname, player_id),
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    if mysqlx.is_duplicate_entry(exc):
                        raise errcode.PandoraError(
                            errcode.ErrPlayerNicknameTaken, "nickname taken: %s", nickname
                        ) from exc
                    raise
                if (cur.rowcount or 0) != 0:
                    return
                # 0 行受影响有两种可能:玩家不存在,或昵称未变。确认玩家是否存在以区分 ——
                # 不区分会把"改成同一个名字"报成 not found,客户端看到一次莫名的失败。
                await cur.execute(
                    "SELECT 1 FROM players WHERE player_id = %s LIMIT 1", (player_id,)
                )
                if await cur.fetchone() is None:
                    raise errcode.PandoraError(
                        errcode.ErrPlayerNotFound, "player not found: %d", player_id
                    )
                # 玩家存在但昵称未变 → 幂等成功
        except asyncio.CancelledError:
            raise
        except errcode.PandoraError:
            raise
        except BaseException as exc:
            raise _internal(f"update nickname player={player_id}", exc) from exc

    # ── 英雄 ──────────────────────────────────────────────────────────────

    async def list_heroes(self, player_id: int) -> list[int]:
        try:
            async with self._cursor() as cur:
                await cur.execute(
                    "SELECT hero_id FROM player_heroes WHERE player_id = %s ORDER BY hero_id",
                    (player_id,),
                )
                rows = await cur.fetchall()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"query heroes player={player_id}", exc) from exc
        return [int(r[0]) for r in rows or ()]

    async def unlock_hero(self, player_id: int, hero_id: int, source: str) -> bool:
        """解锁英雄。已拥有(1062)→ True 幂等命中。"""
        try:
            async with self._cursor() as cur:
                await cur.execute(
                    "INSERT INTO player_heroes (player_id, hero_id, source) VALUES (%s, %s, %s)",
                    (player_id, hero_id, source),
                )
                return False
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            if mysqlx.is_duplicate_entry(exc):
                return True
            raise _internal(f"unlock hero player={player_id} hero={hero_id}", exc) from exc

    async def is_hero_owned(self, player_id: int, hero_id: int) -> bool:
        try:
            async with self._cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM player_heroes WHERE player_id = %s AND hero_id = %s LIMIT 1",
                    (player_id, hero_id),
                )
                return await cur.fetchone() is not None
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"check hero owned player={player_id} hero={hero_id}", exc) from exc

    async def set_active_hero(self, player_id: int, hero_id: int) -> None:
        try:
            async with self._cursor() as cur:
                await cur.execute(
                    "UPDATE players SET active_hero_id = %s WHERE player_id = %s",
                    (hero_id, player_id),
                )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"set active hero player={player_id} hero={hero_id}", exc) from exc

    async def get_active_hero(self, player_id: int) -> int:
        """读出战英雄。未选定 / 未建档 → 0。"""
        try:
            async with self._cursor() as cur:
                await cur.execute(
                    "SELECT active_hero_id FROM players WHERE player_id = %s LIMIT 1",
                    (player_id,),
                )
                row = await cur.fetchone()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"get active hero player={player_id}", exc) from exc
        return int(row[0]) if row is not None else 0

    # ── MMR ───────────────────────────────────────────────────────────────

    async def get_mmr(self, player_id: int, rating_pool: str) -> tuple[int, bool]:
        """读某玩家在某池下的分。found=False 表示该池**没有任何记录**(没打过)。

        刻意不在读路径插占位行:读操作产生写会让只读副本 / 只读事务失败,而且
        "查过一次就算定级了"是错的语义。
        """
        sql = "SELECT mmr FROM player_mmr WHERE player_id = %s AND rating_pool = %s LIMIT 1"
        args: tuple = (player_id, rating_pool)
        if rating_pool == prating.DEFAULT_POOL:
            # expand 期 default 池以旧列为兼容权威:旧副本只会更新 players.mmr,新副本读
            # 该列才能立即看到 Stable 的写入。EXISTS 保留"未打过=false"语义,避免建档时的
            # 1500 兼容默认值被误认为已有段位记录。
            sql = (
                "SELECT p.mmr FROM players AS p WHERE p.player_id = %s AND ("
                "  EXISTS (SELECT 1 FROM player_mmr AS pm"
                "          WHERE pm.player_id = p.player_id AND pm.rating_pool = %s)"
                "  OR EXISTS (SELECT 1 FROM mmr_history AS h"
                "             WHERE h.player_id = p.player_id AND h.rating_pool = %s)"
                ") LIMIT 1"
            )
            args = (player_id, prating.DEFAULT_POOL, prating.DEFAULT_POOL)
        try:
            async with self._cursor() as cur:
                await cur.execute(sql, args)
                row = await cur.fetchone()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"query mmr player={player_id} pool={rating_pool}", exc) from exc
        if row is None:
            return 0, False
        return int(row[0]), True

    async def list_ratings(self, player_id: int) -> list[m.PlayerRating]:
        """列出该玩家**已有记录**的全部池段位分,按 rating_pool 字典序。

        排序保证同一份档案多次查询顺序稳定(客户端可直接渲染)。行数被"每玩家每池至多
        1 行 + 池数由关卡表行数有界"兜住(§9.24 登记豁免),故单次全量返回不分页。
        """
        sql = (
            "SELECT rating_pool, mmr FROM ("
            "  SELECT rating_pool, mmr FROM player_mmr WHERE player_id = %s AND rating_pool <> %s"
            "  UNION ALL"
            "  SELECT %s, p.mmr FROM players AS p"
            "  WHERE p.player_id = %s AND ("
            "    EXISTS (SELECT 1 FROM player_mmr AS pm"
            "            WHERE pm.player_id = p.player_id AND pm.rating_pool = %s)"
            "    OR EXISTS (SELECT 1 FROM mmr_history AS h"
            "               WHERE h.player_id = p.player_id AND h.rating_pool = %s)"
            "  )"
            ") AS ratings ORDER BY rating_pool"
        )
        d = prating.DEFAULT_POOL
        try:
            async with self._cursor() as cur:
                await cur.execute(sql, (player_id, d, d, player_id, d, d))
                rows = await cur.fetchall()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"list ratings player={player_id}", exc) from exc
        return [m.PlayerRating(rating_pool=str(r[0]), mmr=int(r[1])) for r in rows or ()]

    async def apply_mmr_change(self, change: m.MMRChange) -> tuple[int, bool]:
        """幂等改**某个池**的 MMR + 战绩计数。返回 (新 MMR, 是否幂等命中)。

        流程(锁序固定 players → player_mmr → mmr_history,全仓无反向路径):
          ① SELECT players FOR UPDATE —— 本玩家段位写入的**守卫行**(见文件头 ②)
          ② 读该池当前分(default 读旧列兼容权威;其它池无行 = 首战,以 baseline 起算)
          ③ INSERT mmr_history(1062 → 幂等:读回已记录 new_mmr,不重复改任何分)
          ④ upsert player_mmr 到 clamp 后的新分
          ⑤ UPDATE players 的战绩计数;default 池同时更新旧 mmr 投影

        ★ ③ 的幂等键**刻意不把 rating_pool 混进去** —— 一场对局只属于一个池,若并入键,
          同一 match 换个池名就能重复入账。
        """
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    # ① 守卫行:存在性校验 + 该玩家段位写入的互斥点
                    await cur.execute(
                        "SELECT player_id, mmr FROM players WHERE player_id = %s FOR UPDATE",
                        (change.player_id,),
                    )
                    guard = await cur.fetchone()
                    if guard is None:
                        raise errcode.PandoraError(
                            errcode.ErrPlayerNotFound, "player not found: %d", change.player_id
                        )
                    legacy_mmr = int(guard[1])

                    # ② 该池当前分。已被守卫行锁保护,无需再 FOR UPDATE。
                    old_mmr = legacy_mmr
                    if change.rating_pool != prating.DEFAULT_POOL:
                        old_mmr = change.baseline
                        await cur.execute(
                            "SELECT mmr FROM player_mmr WHERE player_id = %s AND rating_pool = %s",
                            (change.player_id, change.rating_pool),
                        )
                        stored = await cur.fetchone()
                        if stored is not None:
                            old_mmr = int(stored[0])

                    new_mmr = old_mmr + change.delta
                    if new_mmr < change.floor:
                        new_mmr = change.floor

                    # ③ 幂等闸:uk (player_id, idempotency_key)
                    try:
                        await cur.execute(
                            "INSERT INTO mmr_history "
                            "(player_id, idempotency_key, rating_pool, delta, reason, old_mmr, new_mmr) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (
                                change.player_id,
                                change.idempotency_key,
                                change.rating_pool,
                                change.delta,
                                change.reason,
                                old_mmr,
                                new_mmr,
                            ),
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:
                        if not mysqlx.is_duplicate_entry(exc):
                            raise _internal(
                                f"insert mmr_history player={change.player_id}", exc
                            ) from exc
                        await cur.execute(
                            "SELECT new_mmr FROM mmr_history "
                            "WHERE player_id = %s AND idempotency_key = %s LIMIT 1",
                            (change.player_id, change.idempotency_key),
                        )
                        recorded = await cur.fetchone()
                        if recorded is None:
                            raise _internal(
                                f"read idem mmr player={change.player_id} "
                                f"key={change.idempotency_key}",
                                exc,
                            ) from exc
                        await conn.commit()
                        return int(recorded[0]), True

                    # ④ 分池落分。首战 INSERT,后续 UPDATE;同一条语句,不做"先查再决定"。
                    await cur.execute(
                        "INSERT INTO player_mmr (player_id, rating_pool, mmr) VALUES (%s, %s, %s) "
                        "ON DUPLICATE KEY UPDATE mmr = VALUES(mmr)",
                        (change.player_id, change.rating_pool, new_mmr),
                    )

                    # ⑤ 战绩计数是**跨池的账号级累计**,不随段位分区。
                    battle_inc = 1 if change.inc_battle else 0
                    win_inc = 1 if change.inc_win else 0
                    if change.rating_pool == prating.DEFAULT_POOL:
                        # 新副本双写 default 投影,让尚未排空的旧副本随后读到同一结果。
                        await cur.execute(
                            "UPDATE players SET mmr = %s, total_battles = total_battles + %s, "
                            "total_wins = total_wins + %s WHERE player_id = %s",
                            (new_mmr, battle_inc, win_inc, change.player_id),
                        )
                    else:
                        await cur.execute(
                            "UPDATE players SET total_battles = total_battles + %s, "
                            "total_wins = total_wins + %s WHERE player_id = %s",
                            (battle_inc, win_inc, change.player_id),
                        )
                await conn.commit()
                return new_mmr, False
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except errcode.PandoraError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except BaseException as exc:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise _internal(f"apply mmr change player={change.player_id}", exc) from exc

    # ── 属性点 ────────────────────────────────────────────────────────────

    async def grant_attribute_points(
        self, player_id: int, points: int, idempotency_key: str
    ) -> tuple[int, bool]:
        """幂等授予可分配点。命中幂等键 → (当前 unspent, True),不重复加。"""
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    try:
                        await cur.execute(
                            "INSERT INTO attr_point_grants (player_id, idempotency_key, points) "
                            "VALUES (%s, %s, %s)",
                            (player_id, idempotency_key, points),
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:
                        if not mysqlx.is_duplicate_entry(exc):
                            raise _internal(f"insert grant player={player_id}", exc) from exc
                        await cur.execute(
                            "SELECT unspent_attr_points FROM players WHERE player_id = %s LIMIT 1",
                            (player_id,),
                        )
                        row = await cur.fetchone()
                        if row is None:
                            raise errcode.PandoraError(
                                errcode.ErrPlayerNotFound, "player not found: %d", player_id
                            ) from exc
                        await conn.commit()
                        return int(row[0]), True

                    await cur.execute(
                        "UPDATE players SET unspent_attr_points = unspent_attr_points + %s "
                        "WHERE player_id = %s",
                        (points, player_id),
                    )
                    if (cur.rowcount or 0) == 0:
                        raise errcode.PandoraError(
                            errcode.ErrPlayerNotFound, "player not found: %d", player_id
                        )
                    await cur.execute(
                        "SELECT unspent_attr_points FROM players WHERE player_id = %s LIMIT 1",
                        (player_id,),
                    )
                    row = await cur.fetchone()
                    unspent = int(row[0]) if row else 0
                await conn.commit()
                return unspent, False
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except errcode.PandoraError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except BaseException as exc:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise _internal(f"grant attr points player={player_id}", exc) from exc

    async def allocate_attribute_points(
        self, player_id: int, allocs: list[m.AttrAllocation]
    ) -> int:
        """分配点(事务:锁 players 行校验 unspent>=sum,扣减,累加 player_attributes)。

        ★ repo **自守**,不依赖上层限制:逐项 checked-add 累计,拒非正点数,校验请求总和、
          单属性列「当前值 + 增量」与 unspent 均不越有符号 INT 列上界。任一越界返回业务
          错误且**零写入**(所有校验都在第一条写之前完成)。
        """
        if not allocs:
            raise errcode.PandoraError(errcode.ErrInvalidArg, "allocations required")
        per_key: dict[str, int] = {}
        total = 0
        for a in allocs:
            if not a.key:
                raise errcode.PandoraError(errcode.ErrInvalidArg, "attr_key must not be empty")
            if a.points <= 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "points must be positive: %s", a.key
                )
            per_key[a.key] = per_key.get(a.key, 0) + a.points
            if per_key[a.key] > MAX_INT32:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "attr %s allocation out of range", a.key
                )
            total += a.points
            if total > MAX_INT32:
                # 总和超过 INT 列上界(必然 >= 任何可能的 unspent)→ 点数不足,零写入。
                raise errcode.PandoraError(
                    errcode.ErrPlayerInsufficientPoints, "total allocation out of range"
                )

        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT unspent_attr_points FROM players WHERE player_id = %s FOR UPDATE",
                        (player_id,),
                    )
                    row = await cur.fetchone()
                    if row is None:
                        raise errcode.PandoraError(
                            errcode.ErrPlayerNotFound, "player not found: %d", player_id
                        )
                    unspent = int(row[0])
                    if total > unspent:
                        raise errcode.PandoraError(
                            errcode.ErrPlayerInsufficientPoints,
                            "insufficient points player=%d need=%d have=%d",
                            player_id,
                            total,
                            unspent,
                        )

                    # 权威列上界:锁定并读取受影响属性当前值,校验「当前值 + 增量」不越界。
                    await cur.execute(
                        "SELECT attr_key, points FROM player_attributes "
                        "WHERE player_id = %s FOR UPDATE",
                        (player_id,),
                    )
                    existing = {str(r[0]): int(r[1]) for r in (await cur.fetchall()) or ()}
                    for key, delta in per_key.items():
                        if existing.get(key, 0) + delta > MAX_INT32:
                            raise errcode.PandoraError(
                                errcode.ErrInvalidArg,
                                "attr %s cumulative points out of range player=%d",
                                key,
                                player_id,
                            )

                    # 按归并后的增量逐属性 upsert(等价原逐条累加,消除重复 key 的列溢出隐患)。
                    for key, delta in per_key.items():
                        await cur.execute(
                            "INSERT INTO player_attributes (player_id, attr_key, points) "
                            "VALUES (%s, %s, %s) "
                            "ON DUPLICATE KEY UPDATE points = points + VALUES(points)",
                            (player_id, key, delta),
                        )
                    new_unspent = unspent - total
                    await cur.execute(
                        "UPDATE players SET unspent_attr_points = %s WHERE player_id = %s",
                        (new_unspent, player_id),
                    )
                await conn.commit()
                return new_unspent
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except errcode.PandoraError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except BaseException as exc:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise _internal(f"allocate attr player={player_id}", exc) from exc

    async def reset_attributes(self, player_id: int) -> int:
        """洗点(事务:锁 players 行,SUM(已分配)退回 unspent,清空 player_attributes)。"""
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT unspent_attr_points FROM players WHERE player_id = %s FOR UPDATE",
                        (player_id,),
                    )
                    row = await cur.fetchone()
                    if row is None:
                        raise errcode.PandoraError(
                            errcode.ErrPlayerNotFound, "player not found: %d", player_id
                        )
                    unspent = int(row[0])
                    await cur.execute(
                        "SELECT COALESCE(SUM(points), 0) FROM player_attributes WHERE player_id = %s",
                        (player_id,),
                    )
                    allocated = int((await cur.fetchone())[0])
                    await cur.execute(
                        "DELETE FROM player_attributes WHERE player_id = %s", (player_id,)
                    )
                    new_unspent = unspent + allocated
                    await cur.execute(
                        "UPDATE players SET unspent_attr_points = %s WHERE player_id = %s",
                        (new_unspent, player_id),
                    )
                await conn.commit()
                return new_unspent
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except errcode.PandoraError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except BaseException as exc:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise _internal(f"reset attr player={player_id}", exc) from exc

    async def get_attributes(self, player_id: int) -> tuple[list[m.AttrPoint], int]:
        try:
            async with self._cursor() as cur:
                await cur.execute(
                    "SELECT attr_key, points FROM player_attributes "
                    "WHERE player_id = %s ORDER BY attr_key",
                    (player_id,),
                )
                rows = await cur.fetchall()
                attrs = [m.AttrPoint(key=str(r[0]), points=int(r[1])) for r in rows or ()]
                await cur.execute(
                    "SELECT unspent_attr_points FROM players WHERE player_id = %s LIMIT 1",
                    (player_id,),
                )
                row = await cur.fetchone()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"query attrs player={player_id}", exc) from exc
        # 未建档 → 可分配点按 0(与 Go 同:不报 not found,读接口不该因懒建档时序而失败)。
        return attrs, int(row[0]) if row is not None else 0

    # ── 出战装备预设 ──────────────────────────────────────────────────────

    async def set_equipment(self, player_id: int, slots: list[m.EquipmentSlot]) -> None:
        """全量替换出战装备预设(事务:删旧 + 按 slot 插新)。

        instance_id=0 在**写路径**一律拒:预设会被 GetLoadout 转成 Battle DS 的初始
        GameplayEffect,配置级(无实例)的预设在同配置多实例时无法确定用哪一件的词条。
        """
        for s in slots:
            if s.instance_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "instance_id required for equipment write player=%d slot=%d",
                    player_id,
                    s.slot,
                )
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await cur.execute(
                        "DELETE FROM player_equipment WHERE player_id = %s", (player_id,)
                    )
                    for s in slots:
                        try:
                            await cur.execute(
                                "INSERT INTO player_equipment "
                                "(player_id, slot, item_config_id, instance_id) "
                                "VALUES (%s, %s, %s, %s)",
                                (player_id, s.slot, s.item_config_id, s.instance_id),
                            )
                        except asyncio.CancelledError:
                            raise
                        except BaseException as exc:
                            if mysqlx.is_duplicate_entry(exc):
                                raise errcode.PandoraError(
                                    errcode.ErrInvalidArg,
                                    "duplicate equipment slot or instance player=%d slot=%d instance=%d",
                                    player_id,
                                    s.slot,
                                    s.instance_id,
                                ) from exc
                            raise
                await conn.commit()
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except errcode.PandoraError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except BaseException as exc:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise _internal(f"set equipment player={player_id}", exc) from exc

    async def get_equipment(self, player_id: int) -> list[m.EquipmentSlot]:
        # instance_id 允许 NULL 仅用于兼容 000006 前旧二进制写下的配置级预设;领域层以 0 表示。
        sql = (
            "SELECT slot, item_config_id, COALESCE(instance_id, 0) FROM player_equipment "
            "WHERE player_id = %s ORDER BY slot"
        )
        try:
            async with self._cursor() as cur:
                await cur.execute(sql, (player_id,))
                rows = await cur.fetchall()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"query equipment player={player_id}", exc) from exc
        return [
            m.EquipmentSlot(slot=int(r[0]), item_config_id=int(r[1]), instance_id=int(r[2]))
            for r in rows or ()
        ]

    # ── 天赋 ──────────────────────────────────────────────────────────────

    async def _talent_unspent(self, cur, player_id: int) -> int:  # noqa: ANN001
        """可点天赋点 = total_talent_points - SUM(已花点数)。未建档 → ErrPlayerNotFound。"""
        await cur.execute(
            "SELECT total_talent_points FROM players WHERE player_id = %s LIMIT 1", (player_id,)
        )
        row = await cur.fetchone()
        if row is None:
            raise errcode.PandoraError(errcode.ErrPlayerNotFound, "player not found: %d", player_id)
        total = int(row[0])
        await cur.execute(
            f"SELECT COALESCE(SUM({TALENT_SPENT_EXPR}), 0) FROM player_talents WHERE player_id = %s",  # noqa: S608
            (player_id,),
        )
        used = int((await cur.fetchone())[0])
        return total - used

    async def grant_talent_points(
        self, player_id: int, points: int, idempotency_key: str
    ) -> tuple[int, bool]:
        """幂等授予天赋点(命中 uk → 读回当前可点,不重复授予)。"""
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    try:
                        await cur.execute(
                            "INSERT INTO talent_point_grants (player_id, idempotency_key, points) "
                            "VALUES (%s, %s, %s)",
                            (player_id, idempotency_key, points),
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:
                        if not mysqlx.is_duplicate_entry(exc):
                            raise _internal(
                                f"insert talent grant player={player_id}", exc
                            ) from exc
                        unspent = await self._talent_unspent(cur, player_id)
                        await conn.commit()
                        return unspent, True

                    await cur.execute(
                        "UPDATE players SET total_talent_points = total_talent_points + %s "
                        "WHERE player_id = %s",
                        (points, player_id),
                    )
                    if (cur.rowcount or 0) == 0:
                        raise errcode.PandoraError(
                            errcode.ErrPlayerNotFound, "player not found: %d", player_id
                        )
                    unspent = await self._talent_unspent(cur, player_id)
                await conn.commit()
                return unspent, False
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except errcode.PandoraError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except BaseException as exc:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise _internal(f"grant talent player={player_id}", exc) from exc

    async def set_talents(self, player_id: int, talents: list[m.TalentLevel]) -> int:
        """全量重置天赋(事务:锁 players 行,校验总消耗<=total,替换 player_talents)。

        每条 spent_points 是 biz 按专精表算好的该节点消耗;这里**不按 sum(level) 推算** ——
        每级消耗是配置表列,repo 看不到配置,自行推算会在 cost_per_level≠1 时算少扣。
        总消耗 = Σ spent_points,与落库的每行消耗同源,不会漂移。
        """
        total_cost = 0
        for t in talents:
            if t.spent_points <= 0:
                # 消耗必须由 biz 按表填;缺失说明调用方绕过了专精表校验,不能按"免费"落库。
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "talent %d missing spent points player=%d",
                    t.talent_id,
                    player_id,
                )
            total_cost += t.spent_points

        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT total_talent_points FROM players WHERE player_id = %s FOR UPDATE",
                        (player_id,),
                    )
                    row = await cur.fetchone()
                    if row is None:
                        raise errcode.PandoraError(
                            errcode.ErrPlayerNotFound, "player not found: %d", player_id
                        )
                    total = int(row[0])
                    if total_cost > total:
                        raise errcode.PandoraError(
                            errcode.ErrPlayerInsufficientPoints,
                            "insufficient talent points player=%d need=%d have=%d",
                            player_id,
                            total_cost,
                            total,
                        )
                    await cur.execute(
                        "DELETE FROM player_talents WHERE player_id = %s", (player_id,)
                    )
                    for t in talents:
                        try:
                            await cur.execute(
                                "INSERT INTO player_talents "
                                "(player_id, talent_id, level, spent_points) VALUES (%s, %s, %s, %s)",
                                (player_id, t.talent_id, t.level, t.spent_points),
                            )
                        except asyncio.CancelledError:
                            raise
                        except BaseException as exc:
                            if mysqlx.is_duplicate_entry(exc):
                                raise errcode.PandoraError(
                                    errcode.ErrInvalidArg,
                                    "duplicate talent_id player=%d talent=%d",
                                    player_id,
                                    t.talent_id,
                                ) from exc
                            raise
                await conn.commit()
                return total - total_cost
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except errcode.PandoraError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except BaseException as exc:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise _internal(f"set talents player={player_id}", exc) from exc

    async def reset_talents(self, player_id: int) -> int:
        """清空天赋(事务:锁 players 行,删 player_talents,可点恢复为 total)。"""
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT total_talent_points FROM players WHERE player_id = %s FOR UPDATE",
                        (player_id,),
                    )
                    row = await cur.fetchone()
                    if row is None:
                        raise errcode.PandoraError(
                            errcode.ErrPlayerNotFound, "player not found: %d", player_id
                        )
                    total = int(row[0])
                    await cur.execute(
                        "DELETE FROM player_talents WHERE player_id = %s", (player_id,)
                    )
                await conn.commit()
                return total
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except errcode.PandoraError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except BaseException as exc:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise _internal(f"reset talents player={player_id}", exc) from exc

    async def get_talents(self, player_id: int) -> tuple[list[m.TalentLevel], int]:
        """读已点天赋 + 可点天赋点。

        已花点数取 TALENT_SPENT_EXPR 而非裸 spent_points:与 _talent_unspent 同一口径,
        共存窗口里老副本写的行(spent_points=0)回退按 level 计。
        """
        sql = (
            f"SELECT talent_id, level, {TALENT_SPENT_EXPR} FROM player_talents "  # noqa: S608
            "WHERE player_id = %s ORDER BY talent_id"
        )
        try:
            async with self._cursor() as cur:
                await cur.execute(sql, (player_id,))
                rows = await cur.fetchall()
                talents = [
                    m.TalentLevel(talent_id=int(r[0]), level=int(r[1]), spent_points=int(r[2]))
                    for r in rows or ()
                ]
                used = sum(t.spent_points for t in talents)
                await cur.execute(
                    "SELECT total_talent_points FROM players WHERE player_id = %s LIMIT 1",
                    (player_id,),
                )
                row = await cur.fetchone()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"query talents player={player_id}", exc) from exc
        if row is None:
            return talents, 0
        return talents, int(row[0]) - used

    # ── 技能卡 ────────────────────────────────────────────────────────────

    @staticmethod
    async def _skill_cards_of(cur, player_id: int) -> list[m.SkillCard]:  # noqa: ANN001
        await cur.execute(
            "SELECT card_id, level, shards FROM player_skill_cards "
            "WHERE player_id = %s ORDER BY card_id",
            (player_id,),
        )
        rows = await cur.fetchall()
        return [
            m.SkillCard(card_id=int(r[0]), level=int(r[1]), shards=int(r[2])) for r in rows or ()
        ]

    async def grant_skill_cards(
        self, player_id: int, grants: list[m.SkillCardGrant], idempotency_key: str
    ) -> tuple[list[m.SkillCard], bool]:
        """幂等发放技能卡 / 碎片。命中幂等键 → (当前持卡, True),不重复入账。

        已持有 → 碎片累加(等级不动:发放不改变培养进度);未持有 → 以 1 级建卡。
        ON DUPLICATE KEY UPDATE 让两条路径走同一条语句:先查再决定 insert/update 会在
        并发发放下丢更新(两个请求都查到"没有",都去 insert,一个失败回滚整批)。
        """
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    try:
                        await cur.execute(
                            "INSERT INTO skill_card_grants (player_id, idempotency_key) "
                            "VALUES (%s, %s)",
                            (player_id, idempotency_key),
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:
                        if not mysqlx.is_duplicate_entry(exc):
                            raise _internal(
                                f"insert skill card grant player={player_id}", exc
                            ) from exc
                        # 幂等命中:本次一张卡一片碎片都不加,只把当前持卡读回去。
                        cards = await self._skill_cards_of(cur, player_id)
                        await conn.commit()
                        return cards, True

                    for g in grants:
                        # level 刻意不写进 UPDATE 子句 —— 发放不该重置已培养的等级。
                        await cur.execute(
                            "INSERT INTO player_skill_cards (player_id, card_id, level, shards) "
                            "VALUES (%s, %s, 1, %s) "
                            "ON DUPLICATE KEY UPDATE shards = shards + VALUES(shards)",
                            (player_id, g.card_id, g.shards),
                        )
                    cards = await self._skill_cards_of(cur, player_id)
                await conn.commit()
                return cards, False
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except errcode.PandoraError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except BaseException as exc:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise _internal(f"grant skill cards player={player_id}", exc) from exc

    async def upgrade_skill_card(
        self, player_id: int, card_id: int, cost_by_level: dict[int, int], max_level: int
    ) -> tuple[m.SkillCard, int]:
        """消耗碎片把一张卡升一级。返回 (升级后状态, 本次消耗)。

        ★ 事务内 FOR UPDATE 锁住卡行:"读余量 → 判够不够 → 扣"三步之间若不加锁,并发
          两次升级能用同一批碎片升两级(§16.1 TOCTOU)。
        ★ 价钱也在锁内按**读到的等级**查曲线,不由调用方预先算好 —— 否则并发两次都会
          按同一级的价钱扣。
        """
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT level, shards FROM player_skill_cards "
                        "WHERE player_id = %s AND card_id = %s FOR UPDATE",
                        (player_id, card_id),
                    )
                    row = await cur.fetchone()
                    if row is None:
                        raise errcode.PandoraError(
                            errcode.ErrSkillCardNotOwned,
                            "skill card not owned player=%d card=%d",
                            player_id,
                            card_id,
                        )
                    level, shards = int(row[0]), int(row[1])
                    if level >= max_level:
                        raise errcode.PandoraError(
                            errcode.ErrSkillCardMaxLevel,
                            "skill card at max level player=%d card=%d level=%d max=%d",
                            player_id,
                            card_id,
                            level,
                            max_level,
                        )
                    target_level = level + 1
                    shard_cost = cost_by_level.get(target_level)
                    if shard_cost is None:
                        # 曲线断档。绝不能当免费升级放行 —— 加载期 ValidateCurves 已挡过
                        # 一道,走到这里说明表和上限对不上,宁可拒绝也不给玩家白升。
                        raise errcode.PandoraError(
                            errcode.ErrInternal,
                            "upgrade curve missing level player=%d card=%d level=%d",
                            player_id,
                            card_id,
                            target_level,
                        )
                    if shards < shard_cost:
                        raise errcode.PandoraError(
                            errcode.ErrSkillCardInsufficientShards,
                            "insufficient shards player=%d card=%d need=%d have=%d",
                            player_id,
                            card_id,
                            shard_cost,
                            shards,
                        )
                    await cur.execute(
                        "UPDATE player_skill_cards SET level = level + 1, shards = shards - %s "
                        "WHERE player_id = %s AND card_id = %s",
                        (shard_cost, player_id, card_id),
                    )
                await conn.commit()
                return (
                    m.SkillCard(card_id=card_id, level=target_level, shards=shards - shard_cost),
                    shard_cost,
                )
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except errcode.PandoraError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except BaseException as exc:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise _internal(
                    f"upgrade skill card player={player_id} card={card_id}", exc
                ) from exc

    async def set_skill_slots(self, player_id: int, slots: list[m.SkillSlot]) -> None:
        """全量替换卡槽装配(未列出的槽视为清空)。

        先校验每张要装的卡都真的持有:装一张没有的卡在库层面不会报错(卡槽表不带外键),
        结果是开局给不出技能且无从排查。持有校验在**事务内**做(与删旧插新同一把锁),
        不预查 —— 预查会引入"查到持有 → 期间卡被消耗 → 装上了没有的卡"的窗口。
        """
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    for s in slots:
                        await cur.execute(
                            "SELECT 1 FROM player_skill_cards WHERE player_id = %s AND card_id = %s",
                            (player_id, s.card_id),
                        )
                        if await cur.fetchone() is None:
                            raise errcode.PandoraError(
                                errcode.ErrSkillCardNotOwned,
                                "skill card not owned player=%d card=%d slot=%d",
                                player_id,
                                s.card_id,
                                s.slot,
                            )
                    await cur.execute(
                        "DELETE FROM player_skill_slots WHERE player_id = %s", (player_id,)
                    )
                    for s in slots:
                        try:
                            await cur.execute(
                                "INSERT INTO player_skill_slots (player_id, slot, card_id) "
                                "VALUES (%s, %s, %s)",
                                (player_id, s.slot, s.card_id),
                            )
                        except asyncio.CancelledError:
                            raise
                        except BaseException as exc:
                            if mysqlx.is_duplicate_entry(exc):
                                # uk_player_slot 或 uk_player_card_once 撞了。biz 侧已校验过
                                # 一次,能走到这里说明是并发两次 SetSkillSlots 交错 ——
                                # 库是最后一道。
                                raise errcode.PandoraError(
                                    errcode.ErrSkillCardSlotInvalid,
                                    "duplicate slot or card player=%d slot=%d card=%d",
                                    player_id,
                                    s.slot,
                                    s.card_id,
                                ) from exc
                            raise
                await conn.commit()
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except errcode.PandoraError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except BaseException as exc:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise _internal(f"set skill slots player={player_id}", exc) from exc

    async def get_skill_cards(self, player_id: int) -> list[m.SkillCard]:
        try:
            async with self._cursor() as cur:
                return await self._skill_cards_of(cur, player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"query skill cards player={player_id}", exc) from exc

    async def get_skill_slots(self, player_id: int) -> list[m.SkillSlot]:
        try:
            async with self._cursor() as cur:
                await cur.execute(
                    "SELECT slot, card_id FROM player_skill_slots WHERE player_id = %s ORDER BY slot",
                    (player_id,),
                )
                rows = await cur.fetchall()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"query skill slots player={player_id}", exc) from exc
        return [m.SkillSlot(slot=int(r[0]), card_id=int(r[1])) for r in rows or ()]

    # ── 玩家等级经验(实时成长)────────────────────────────────────────────

    async def apply_experience(self, apply: m.ExpApply) -> tuple[m.ExpState, bool]:
        """幂等入账经验并结算等级,与经验推送出箱**同一事务**原子提交(不变量 §4)。

        事务顺序(锁序固定,防死锁):锁 players 行 → 满级判定 → INSERT exp_history(幂等)
        → UPDATE players → INSERT player_push_outbox。

        ★ 满级 no-op:不加经验、不出箱,但**仍消费幂等键**落 no-op 收据。若不落收据,
          成功响应丢失 + 未来曲线扩容(上限提升)后,滞留在上游出箱的同一事件重试会被
          重新入账,破坏 exactly-once。重放命中收据按契约返回 already=True。
        """
        max_level = len(apply.curve) + 1
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT level, exp FROM players WHERE player_id = %s FOR UPDATE",
                        (apply.player_id,),
                    )
                    row = await cur.fetchone()
                    if row is None:
                        raise errcode.PandoraError(
                            errcode.ErrPlayerNotFound, "player not found: %d", apply.player_id
                        )
                    level, exp = int(row[0]), int(row[1])

                    if level >= max_level:
                        already_consumed = False
                        try:
                            await cur.execute(
                                "INSERT INTO exp_history "
                                "(player_id, idempotency_key, exp_delta, reason, "
                                " old_level, old_exp, new_level, new_exp) "
                                "VALUES (%s, %s, %s, %s, %s, 0, %s, 0)",
                                (
                                    apply.player_id,
                                    apply.idempotency_key,
                                    apply.delta,
                                    apply.reason,
                                    max_level,
                                    max_level,
                                ),
                            )
                        except asyncio.CancelledError:
                            raise
                        except BaseException as exc:
                            if not mysqlx.is_duplicate_entry(exc):
                                raise _internal(
                                    f"insert max-level noop receipt player={apply.player_id}", exc
                                ) from exc
                            already_consumed = True
                        await conn.commit()
                        return (
                            m.ExpState(level=max_level, exp_in_level=0, is_max_level=True),
                            already_consumed,
                        )

                    new_level, new_exp, gained = advance_experience(
                        level, exp, apply.delta, list(apply.curve)
                    )

                    try:
                        await cur.execute(
                            "INSERT INTO exp_history "
                            "(player_id, idempotency_key, exp_delta, reason, "
                            " old_level, old_exp, new_level, new_exp) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                            (
                                apply.player_id,
                                apply.idempotency_key,
                                apply.delta,
                                apply.reason,
                                level,
                                exp,
                                new_level,
                                new_exp,
                            ),
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:
                        if not mysqlx.is_duplicate_entry(exc):
                            raise _internal(
                                f"insert exp history player={apply.player_id}", exc
                            ) from exc
                        # 幂等命中:回读**锁内**当前权威快照(原次入账已生效),
                        # 不重复加、不重复出箱。
                        await conn.commit()
                        return (
                            m.ExpState(
                                level=level, exp_in_level=exp, is_max_level=level >= max_level
                            ),
                            True,
                        )

                    await cur.execute(
                        "UPDATE players SET level = %s, exp = %s WHERE player_id = %s",
                        (new_level, new_exp, apply.player_id),
                    )

                    # 经验推送出箱:与入账同事务。payload 是入账后的**权威快照**,
                    # push 经 pandora.player.experience(event_type=EXPERIENCE)透传给客户端。
                    now_ms = int(time.time() * 1000)
                    evt = ppb.PlayerExperienceEvent(
                        player_id=apply.player_id,
                        level=new_level,
                        exp_in_level=new_exp,
                        is_max_level=new_level >= max_level,
                        levels_gained=gained,
                        ts_ms=now_ms,
                    )
                    await cur.execute(
                        "INSERT INTO player_push_outbox "
                        "(player_id, event_type, payload, created_at_ms) VALUES (%s, %s, %s, %s)",
                        (
                            apply.player_id,
                            int(ppb.PLAYER_PUSH_EVENT_TYPE_EXPERIENCE),
                            evt.SerializeToString(),
                            now_ms,
                        ),
                    )
                await conn.commit()
                return (
                    m.ExpState(
                        level=new_level,
                        exp_in_level=new_exp,
                        is_max_level=new_level >= max_level,
                        levels_gained=gained,
                    ),
                    False,
                )
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except errcode.PandoraError:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            except BaseException as exc:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise _internal(f"apply experience player={apply.player_id}", exc) from exc

    async def fetch_push_outbox(self, limit: int) -> list[m.PushOutboxRecord]:
        """按 id 升序取最多 limit 条待发布记录(FIFO 保序,同玩家事件有序)。"""
        if limit <= 0:
            limit = 128
        try:
            async with self._cursor() as cur:
                await cur.execute(
                    "SELECT id, player_id, event_type, payload FROM player_push_outbox "
                    "ORDER BY id ASC LIMIT %s",
                    (limit,),
                )
                rows = await cur.fetchall()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal("query push outbox", exc) from exc
        return [
            m.PushOutboxRecord(
                id=int(r[0]), player_id=int(r[1]), event_type=int(r[2]), payload=bytes(r[3])
            )
            for r in rows or ()
        ]

    async def delete_push_outbox(self, record_id: int) -> None:
        try:
            async with self._cursor() as cur:
                await cur.execute(
                    "DELETE FROM player_push_outbox WHERE id = %s", (record_id,)
                )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"delete push outbox id={record_id}", exc) from exc

    # ── 保留期清理(§9.24)────────────────────────────────────────────────

    async def _sweep_by_created_at(
        self, mode: dbguard.Mode, table: str, cutoff: _dt.datetime, limit: int
    ) -> dbguard.Outcome:
        """按 created_at < cutoff 处理指定表(表名只来自本文件内固定调用点,非外部输入)。

        走 dbguard.sweep_table 而不是自己写 COUNT / DELETE 两条 SQL:它强制"报告"与
        "实删"共用同一个 where + 同一组参数,从机制上排除"报告说 0 行、实际删了 10 万行"
        的条件漂移;WARN 日志与 pending gauge 也由它统一打,全服清理告警长一个样。
        """
        if limit <= 0:
            limit = 1000
        try:
            async with self._pool.acquire() as conn:
                return await dbguard.sweep_table(
                    conn, mode, self._schema, table, "created_at < %s", limit, cutoff
                )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"sweep {table}", exc) from exc

    async def sweep_exp_history(self, mode, cutoff, limit):  # noqa: ANN001, ANN201
        return await self._sweep_by_created_at(mode, "exp_history", cutoff, limit)

    async def sweep_mmr_history(self, mode, cutoff, limit):  # noqa: ANN001, ANN201
        return await self._sweep_by_created_at(mode, "mmr_history", cutoff, limit)

    async def sweep_attr_point_grants(self, mode, cutoff, limit):  # noqa: ANN001, ANN201
        return await self._sweep_by_created_at(mode, "attr_point_grants", cutoff, limit)

    async def sweep_talent_point_grants(self, mode, cutoff, limit):  # noqa: ANN001, ANN201
        return await self._sweep_by_created_at(mode, "talent_point_grants", cutoff, limit)

    async def sweep_skill_card_grants(self, mode, cutoff, limit):  # noqa: ANN001, ANN201
        return await self._sweep_by_created_at(mode, "skill_card_grants", cutoff, limit)

    # ── 领奖记录 ──────────────────────────────────────────────────────────

    async def load_reward_claims(self, player_id: int) -> tuple[bytes, int]:
        """读领奖记录(序列化 bytes + 乐观锁版本)。未建行 → (b"", 0)。"""
        try:
            async with self._cursor() as cur:
                await cur.execute(
                    "SELECT record, version FROM player_reward_claims WHERE player_id = %s LIMIT 1",
                    (player_id,),
                )
                row = await cur.fetchone()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"load reward claims player={player_id}", exc) from exc
        if row is None:
            return b"", 0
        return bytes(row[0] or b""), int(row[1])

    async def save_reward_claims(
        self, player_id: int, record: bytes, expect_version: int
    ) -> None:
        """乐观锁写:expect_version==0 → INSERT;>0 → UPDATE ... WHERE version。

        版本不匹配 / 并发冲突 → ErrPlayerVersionMismatch(由 biz 决定是否重试)。
        """
        if expect_version == 0:
            try:
                async with self._cursor() as cur:
                    await cur.execute(
                        "INSERT INTO player_reward_claims (player_id, record, version) "
                        "VALUES (%s, %s, 1)",
                        (player_id, record),
                    )
                return
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                if mysqlx.is_duplicate_entry(exc):
                    raise errcode.PandoraError(
                        errcode.ErrPlayerVersionMismatch,
                        "reward claims player=%d already exists (expect new)",
                        player_id,
                    ) from exc
                raise _internal(f"insert reward claims player={player_id}", exc) from exc

        try:
            async with self._cursor() as cur:
                await cur.execute(
                    "UPDATE player_reward_claims SET record = %s, version = version + 1 "
                    "WHERE player_id = %s AND version = %s",
                    (record, player_id, expect_version),
                )
                affected = cur.rowcount or 0
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _internal(f"update reward claims player={player_id}", exc) from exc
        if affected == 0:
            raise errcode.PandoraError(
                errcode.ErrPlayerVersionMismatch,
                "reward claims player=%d version mismatch (expect %d)",
                player_id,
                expect_version,
            )

    # ── 启动 schema 闸(fail-fast,§16.4)──────────────────────────────────

    async def validate_experience_schema(self) -> None:
        """探测经验相关表列 + 幂等唯一索引形态,缺失即失败。

        副本不能先 Ready 再在首个 GetProfile / AddExperience 上大面积报错(迁移顺序
        错误要在发布时拦住)。

        ★ 唯一索引探测是**必需**的,不是锦上添花:列探测通过但 uk 缺失(手工建表漂移)时
          1062 永不触发,重试直接双发。而且必须核对**列名、顺序与 SUB_PART**:
          - 同名错列的 UNIQUE(id, idempotency_key) 也有两列,列名不比对就放过去了;
          - 前缀唯一索引 UNIQUE(player_id, idempotency_key(1)) 列名顺序全对,却会把
            首字符相同的不同幂等键判成 duplicate,**静默少发经验**——SUB_PART 必须为 NULL。
        """
        checks = (
            "SELECT player_id, level, exp FROM players LIMIT 0",
            "SELECT id, player_id, idempotency_key, exp_delta, reason, old_level, old_exp, "
            "new_level, new_exp, created_at FROM exp_history LIMIT 0",
            "SELECT id, player_id, event_type, payload, created_at_ms FROM player_push_outbox LIMIT 0",
        )
        try:
            async with self._cursor() as cur:
                for query in checks:
                    await cur.execute(query)
                    await cur.fetchall()

                await cur.execute(
                    "SELECT SEQ_IN_INDEX, COLUMN_NAME, SUB_PART FROM information_schema.STATISTICS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'exp_history' "
                    "  AND INDEX_NAME = 'uk_player_idem' AND NON_UNIQUE = 0 "
                    "ORDER BY SEQ_IN_INDEX"
                )
                rows = await cur.fetchall()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, "player experience schema invalid: %s", exc
            ) from exc

        cols: list[str] = []
        for seq, name, sub_part in rows or ():
            if int(seq) != len(cols) + 1:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "exp_history uk_player_idem column sequence broken at %d (got seq %d)",
                    len(cols) + 1,
                    int(seq),
                )
            if sub_part is not None:
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "exp_history uk_player_idem column %s is a prefix index (SUB_PART=%s): "
                    "full-column uniqueness required, idempotency broken",
                    str(name),
                    sub_part,
                )
            cols.append(str(name))
        if cols != ["player_id", "idempotency_key"]:
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "exp_history uk_player_idem missing or malformed (columns=%s, "
                "want ['player_id', 'idempotency_key']): idempotency broken",
                cols,
            )

    async def validate_equipment_schema(self) -> None:
        """确认 000006 已落地且唯一键形态正确(instance_id 列 + uk_player_instance)。

        新二进制的读写 SQL 都引用 instance_id;等首个玩家请求才暴露漏迁移会形成部分 Ready。
        """
        try:
            async with self._cursor() as cur:
                await cur.execute(
                    "SELECT DATA_TYPE, COLUMN_TYPE, IS_NULLABLE FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_equipment' "
                    "  AND COLUMN_NAME = 'instance_id'"
                )
                col = await cur.fetchone()
                if col is None:
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "player equipment instance_id schema invalid: column not found",
                    )
                data_type, column_type, nullable = str(col[0]), str(col[1]), str(col[2])
                if (
                    data_type.lower() != "bigint"
                    or "unsigned" not in column_type.lower()
                    or nullable != "YES"
                ):
                    raise errcode.PandoraError(
                        errcode.ErrInternal,
                        "player equipment instance_id malformed data_type=%s column_type=%s nullable=%s",
                        data_type,
                        column_type,
                        nullable,
                    )

                await cur.execute(
                    "SELECT SEQ_IN_INDEX, COLUMN_NAME, SUB_PART FROM information_schema.STATISTICS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_equipment' "
                    "  AND INDEX_NAME = 'uk_player_instance' AND NON_UNIQUE = 0 "
                    "ORDER BY SEQ_IN_INDEX"
                )
                rows = await cur.fetchall()
        except asyncio.CancelledError:
            raise
        except errcode.PandoraError:
            raise
        except BaseException as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, "probe player equipment instance unique index: %s", exc
            ) from exc

        want = ("player_id", "instance_id")
        index = 0
        for seq, name, sub_part in rows or ():
            if (
                index >= len(want)
                or int(seq) != index + 1
                or str(name) != want[index]
                or sub_part is not None
            ):
                raise errcode.PandoraError(
                    errcode.ErrInternal,
                    "player equipment uk_player_instance malformed at index=%d seq=%s column=%s prefix=%s",
                    index,
                    seq,
                    name,
                    sub_part is not None,
                )
            index += 1
        if index != len(want):
            raise errcode.PandoraError(
                errcode.ErrInternal,
                "player equipment uk_player_instance missing columns=%d want=%d",
                index,
                len(want),
            )

    async def validate_experience_levels(self, max_level: int) -> None:
        """确认持久化等级落在当前策划表范围内。

        热更另由 tables.load_tables 的 current_max_level 基线禁止缩短最高等级,因此通过
        本闸后不会因换表把玩家降级。缺了本闸:一份比库里最高等级还短的表能被首载接受,
        高等级玩家在下一次 AddExperience 被按新上限重新结算,等级凭空掉下去。
        """
        if max_level < 1:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "player level table max_level invalid: %d", max_level
            )
        try:
            async with self._cursor() as cur:
                await cur.execute(
                    "SELECT COALESCE(MIN(level), 1), COALESCE(MAX(level), 1) FROM players"
                )
                row = await cur.fetchone()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise errcode.PandoraError(
                errcode.ErrInternal, "validate player level range: %s", exc
            ) from exc
        min_level, stored_max = int(row[0]), int(row[1])
        if min_level < 1 or stored_max > max_level:
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "players.level range [%d,%d] outside config range [1,%d]",
                min_level,
                stored_max,
                max_level,
            )
