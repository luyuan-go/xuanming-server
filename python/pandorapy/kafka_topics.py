"""Kafka topic 常量 —— **由 tools/gen_kafka_topics.py 从 pkg/kafkax/topics.go 生成，勿手改。**

改 topic 名请改 Go 侧那份，然后重跑生成器；CI 有 `--check` 门禁盯着两边一致。

为什么要机械同步：topic 名是 producer 与 consumer 唯一的约会地点，写错一个字符
**两侧都不报错**（Kafka 自动建 topic，生产成功，消费端只是"没有消息"）——
事实静默永久丢失，日志里一行异常都没有。
"""

from __future__ import annotations

# TopicTeamUpdate — proto: pandora.team.v1.TeamUpdateEvent
# key=player_id;原则 2:不发给发起方
TOPIC_TEAM_UPDATE = "pandora.team.update"  # Go: kafkax.TopicTeamUpdate

# TopicMatchProgress — proto: pandora.match.v1.MatchProgressEvent
# key=player_id;**原则 3 例外**:stage 异步变化必须发给所有人(含发起方)
TOPIC_MATCH_PROGRESS = "pandora.match.progress"  # Go: kafkax.TopicMatchProgress

# TopicChatWorld — proto: pandora.chat.v1.ChatPushEvent
# 全服广播(key 暂留空,push 服务侧 Broadcast 路由,W3 ④ 暂不订阅)
TOPIC_CHAT_WORLD = "pandora.chat.world"  # Go: kafkax.TopicChatWorld

# TopicChatTeam — proto: pandora.chat.v1.ChatPushEvent
# key=player_id;原则 2:只发收件方
TOPIC_CHAT_TEAM = "pandora.chat.team"  # Go: kafkax.TopicChatTeam

# TopicChatPrivate — proto: pandora.chat.v1.ChatPushEvent
# key=player_id;原则 2:只发接收方
TOPIC_CHAT_PRIVATE = "pandora.chat.private"  # Go: kafkax.TopicChatPrivate

# TopicChatGuild — proto: pandora.chat.v1.ChatPushEvent
# key=接收方 player_id;原则 2:只发收件方(公会频道 fan-out,即时不落库)
TOPIC_CHAT_GUILD = "pandora.chat.guild"  # Go: kafkax.TopicChatGuild

# TopicChatGroup — proto: pandora.chat.v1.ChatPushEvent
# key=接收方 player_id;原则 2:只发收件方(临时群频道 fan-out,即时不落库)
TOPIC_CHAT_GROUP = "pandora.chat.group"  # Go: kafkax.TopicChatGroup

# TopicPlayerUpdate — proto: pandora.player.v1.PlayerUpdateEvent(W3+ 补)
# key=player_id;玩家档案变更通知(MMR/昵称/英雄池)。
#
# ⚠️ 单事件类型 topic(金丝雀混跑安全,不变量 §21):player 服务旧副本消费本 topic 时
# 不看 event_type header,直接按 PlayerUpdateEvent 解码做 MMR 入账 —— 在共存窗口里
# 任何"同 topic 加新 event_type"的消息都会被旧副本误解码(字段 2/3 恰好能对上
# match_id/mmr_delta,静默污染 MMR)。因此本 topic 永远只承载 PlayerUpdateEvent;
# player 域新增事件一律开新 topic(如 TopicPlayerExperience)。
TOPIC_PLAYER_UPDATE = "pandora.player.update"  # Go: kafkax.TopicPlayerUpdate

# TopicPlayerExperience — proto: pandora.player.v1.PlayerExperienceEvent(实时成长,2026-07-21)
# key=player_id;player 服务经验入账后出箱生产,push 订阅透传客户端刷新经验条/播升级表现。
# event_type header 恒为 PLAYER_PUSH_EVENT_TYPE_EXPERIENCE(客户端按 (topic, event_type) 选型)。
# 独立 topic 的原因见 TopicPlayerUpdate 注释(旧 player 副本不订阅本 topic,混跑零风险)。
TOPIC_PLAYER_EXPERIENCE = "pandora.player.experience"  # Go: kafkax.TopicPlayerExperience

# TopicFriendEvent — proto: pandora.friend.v1.FriendEvent
# key=to_player_id;原则 2:发给接收方(好友请求 / 接受通知)
TOPIC_FRIEND_EVENT = "pandora.friend.event"  # Go: kafkax.TopicFriendEvent

# TopicGuildEvent — proto: pandora.guild.v1.GuildEvent
# key=to_player_id;原则 2:发给接收方(公会申请 / 审批 / 踢人 / 解散通知)
TOPIC_GUILD_EVENT = "pandora.guild.event"  # Go: kafkax.TopicGuildEvent

# TopicSystemNotify — proto: pandora.system.v1.SystemNotifyEvent(W3+ 补)
# 广播类(key 可空);系统公告 / 邮件红点 / 运营推送
TOPIC_SYSTEM_NOTIFY = "pandora.system.notify"  # Go: kafkax.TopicSystemNotify

# TopicHubMigrate — proto: pandora.hub.v1.HubMigrateEvent
# key=player_id;原则 2 例外:强制整合(缩容排空)时把「新分片地址+新 hub 票据+倒计时」
# 推给被迁移玩家本人,客户端倒计时到点重连新大厅(与 Hub DS drain 心跳指令双通道)
TOPIC_HUB_MIGRATE = "pandora.hub.migrate"  # Go: kafkax.TopicHubMigrate

# TopicPresenceUpdate — proto: pandora.locator.v1.PresenceBatchEvent
# key=subscriber_id;好友在线态订阅推送(docs/design/friend-distributed-scaling.md §13.4)。
# player_locator 的 fan-out worker 去抖+合并后,把「你关注的好友 A/C/F 上线了」
# 批量推给订阅者本人;push 服务按 key=subscriber_id 路由到其 stream。
TOPIC_PRESENCE_UPDATE = "pandora.presence.update"  # Go: kafkax.TopicPresenceUpdate

# TopicMissionUpdate — proto: pandora.mission.v1.MissionUpdateEvent(任务域,2026-08-11)
# key=player_id;**原则 3 例外**:任务进度由 battle_result 出箱等异步事实驱动,
# 状态机变化必须发给玩家本人。mission 服务事务出箱生产;推送不承担正确性
# (原则 5):客户端判重按 mission_config_id + 状态,resync 回源 ListMissions。
# 独立单事件类型 topic(金丝雀混跑纪律,见 TopicPlayerUpdate 注释)。
TOPIC_MISSION_UPDATE = "pandora.mission.update"  # Go: kafkax.TopicMissionUpdate

# TopicBattleResult — proto: pandora.battle.v1.BattleResult
# key=match_id;at-least-once,消费者(battle_result)幂等落库(不变量 §2)
TOPIC_BATTLE_RESULT = "pandora.battle.result"  # Go: kafkax.TopicBattleResult

# TopicDSLifecycle — proto: pandora.ds.v1.DSLifecycleEvent
# key=match_id;W4 ③ ds_allocator 心跳超时发 ABANDONED → battle_result 写补偿记录(不变量 §4)
TOPIC_DS_LIFECYCLE = "pandora.ds.lifecycle"  # Go: kafkax.TopicDSLifecycle

# TopicPlayerPresence — proto: pandora.locator.v1.PlayerLeftHubEvent
# key=player_id;player_locator 在 ReportDisconnect 成功缩 TTL 时生产。
#
# ⚠️ 语义是「离开了 Hub」而**不是「下线」**(travel 去战斗、秒重连都会产生),
# 消费者只能把它当触发器,到自己的阈值后回查 locator 权威再动作(见 proto 注释)。
# 供 pkg/offlinewatch 的通用消费骨架订阅;push 不订阅,不下发客户端。
#
# ⚠️ 勿与 TopicPresenceUpdate 混用:那条是给好友面板的粗粒度状态批量推送
# (key=subscriber_id、单实例内存订阅 + 去抖、可 killswitch 丢弃),
# 投递保证与语义都不同,混用会让「可丢的展示流」和「要兜底的业务触发流」互相污染。
TOPIC_PLAYER_PRESENCE = "pandora.player.presence"  # Go: kafkax.TopicPlayerPresence

# push 服务默认订阅的 topic 集合（Go: kafkax.PushTopics，顺序一并对齐）。
PUSH_TOPICS: tuple[str, ...] = (
    TOPIC_TEAM_UPDATE,
    TOPIC_MATCH_PROGRESS,
    TOPIC_CHAT_PRIVATE,
    TOPIC_CHAT_TEAM,
    TOPIC_CHAT_WORLD,
    TOPIC_CHAT_GUILD,
    TOPIC_CHAT_GROUP,
    TOPIC_HUB_MIGRATE,
    TOPIC_FRIEND_EVENT,
    TOPIC_GUILD_EVENT,
    TOPIC_PRESENCE_UPDATE,
    TOPIC_PLAYER_EXPERIENCE,
    TOPIC_MISSION_UPDATE,
)

# 广播类：kafka key 为空，消费侧必须 Broadcast（Go: kafkax.BroadcastTopics）。
BROADCAST_TOPICS: frozenset[str] = frozenset(
    {
        TOPIC_CHAT_WORLD,
        TOPIC_SYSTEM_NOTIFY,
    }
)


def build_dlq_topic(original_topic: str) -> str:
    """构造死信队列 topic（infra.md §4.4）。对应 Go 的 kafkax.BuildDLQTopic。

        build_dlq_topic("pandora.battle.result") → "pandora.dlq.battle.result"

    注意 Go 侧的实现（config.go:543-549）对**不带** `pandora.` 前缀的输入是直接拼，
    不是原样返回；这里逐分支照搬，不"改进"。
    """
    prefix = "pandora."
    if len(original_topic) > len(prefix) and original_topic.startswith(prefix):
        return "pandora.dlq." + original_topic[len(prefix) :]
    return "pandora.dlq." + original_topic


def is_broadcast_topic(topic: str) -> bool:
    """是否广播类（kafka key 为空，消费侧必须走 Broadcast 而不是按 player_id 路由）。

    判错的后果不对称：把广播类当定向 → 空 key 解析 player_id 失败，消息被当
    invalid key ack 掉，**全服公告静默不达**；反过来则是把定向消息广播给所有人。
    """
    return topic in BROADCAST_TOPICS
