# GitHub 游戏源码与开发资源目录

资料整理日期：2026-09-09。

本目录共收录 **152 个唯一仓库标识**，覆盖游戏源码、经典游戏重实现、客户端与服务端、配套资源、引擎、工具、算法、资料合集及检索中发现的受限候选。收录范围包含 GitHub game Topics 页面摘录的 80 个仓库，以及此前游戏、棋牌、卡牌与捕鱼调研中的相关仓库；按 owner/repo 忽略大小写去重。

## 收录与核对口径

- **仓库核对／候选核对**：根据仓库首页、README、可见文件或 GitHub API 记录；不代表已经编译运行。
- **Topics 摘录**：依据 Topics 页面提供的项目描述分类，本次未逐项独立复核维护状态、资源完整性和许可证。
- **低星搜索核对**：保留最初检索命中的低星示例，依据 GitHub 搜索结果或仓库页面摘要记录，未作深入评估。
- **配套引用**：从已核对项目的 README 引出的客户端、服务端或资源库。
- **Star** 是资料查询时的快照，部分页面可能使用索引缓存；带 k 的为约数，1k = 1000。**— 表示未记录，不表示 0**。
- “重实现”“引擎”“服务端”“部署包”“资源合集”分别标明源码范围；不将它们一律称为完整可运行游戏。表内未写限制的项目，也不表示已经完成完整性或许可证审计。

## 分类索引

| 分类 | 仓库数 |
|---|---:|
| [策略、战棋、塔防](#category-1) | 9 |
| [模拟经营、沙盒、建造](#category-2) | 12 |
| [RPG、生存、冒险](#category-3) | 7 |
| [MMO 服务端与经典重实现](#category-4) | 8 |
| [动作、射击、竞速、音乐](#category-5) | 15 |
| [卡牌、棋牌、麻将](#category-6) | 13 |
| [休闲小游戏](#category-7) | 5 |
| [捕鱼](#category-8) | 3 |
| [编程教育与引擎示例](#category-9) | 7 |
| [游戏引擎与客户端框架](#category-10) | 15 |
| [网络、ECS与开发工具](#category-11) | 12 |
| [资料导航与资源合集](#category-12) | 9 |
| [配套客户端、服务端与资源](#category-13) | 5 |
| [算法、AI与规则库](#category-14) | 4 |
| [备选与范围受限项目](#category-15) | 12 |
| [运行环境、模拟器与运维工具](#category-16) | 5 |
| [关联但非游戏项目](#category-17) | 1 |
| [低星纸牌与消除示例](#category-18) | 10 |

## 继续检索的入口

- [GitHub game：按 Star 降序](https://github.com/topics/game?o=desc&s=stars)
- [RPG](https://github.com/topics/rpg?o=desc&s=stars)、[Roguelike](https://github.com/topics/roguelike?o=desc&s=stars)、[MMORPG](https://github.com/topics/mmorpg?o=desc&s=stars)
- [策略游戏](https://github.com/topics/strategy-game?o=desc&s=stars)、[模拟](https://github.com/topics/simulation?o=desc&s=stars)、[FPS](https://github.com/topics/fps?o=desc&s=stars)、[HTML5 游戏](https://github.com/topics/html5-games?o=desc&s=stars)

<a id="category-1"></a>

## 策略、战棋、塔防（9 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [Mindustry](https://github.com/Anuken/Mindustry)<br>Anuken/Mindustry | 28,929 | Java | 工厂自动化＋塔防＋RTS；游戏及独立服务端 | 仓库核对 |
| [OpenRA](https://github.com/OpenRA/OpenRA)<br>OpenRA/OpenRA | 17,350 | C# | 红警等经典 RTS 重实现；首次下载或导入素材 | 仓库核对 |
| [Unciv](https://github.com/yairm210/Unciv)<br>yairm210/Unciv | 11,251 | Kotlin | 文明风格 4X；桌面和 Android 游戏 | 仓库核对 |
| [Wesnoth](https://github.com/wesnoth/wesnoth)<br>wesnoth/wesnoth | 6,869 | C++ / Lua | 奇幻回合制战棋；战役、AI、地图编辑、联机 | 仓库核对 |
| [Warzone 2100](https://github.com/Warzone2100/warzone2100)<br>Warzone2100/warzone2100 | 3,942 | C++ / JS | 科幻 3D RTS；科技树、单位组装、多人对战 | 仓库核对 |
| [0 A.D.](https://github.com/0ad/0ad)<br>0ad/0ad | 2,821 | C++ / JS | 古代战争 RTS；GitHub 镜像已归档，开发迁至官方 Gitea | 仓库核对 |
| [Freeciv](https://github.com/freeciv/freeciv)<br>freeciv/freeciv | 1,590 | C | 文明风格回合策略；客户端＋服务端 | 仓库核对 |
| [FreeOrion](https://github.com/freeorion/freeorion)<br>freeorion/freeorion | 1,039 | C++ / Python | 太空帝国经营与银河征服 4X | 仓库核对 |
| [FreeCol](https://github.com/FreeCol/freecol)<br>FreeCol/freecol | 709 | Java | 殖民经营与回合策略，含联机 | 仓库核对 |

<a id="category-2"></a>

## 模拟经营、沙盒、建造（12 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [OpenRCT2](https://github.com/OpenRCT2/OpenRCT2)<br>OpenRCT2/OpenRCT2 | 16,198 | C++ | 过山车大亨 2 重实现；需原版 RCT2 资源 | 仓库核对 |
| [Luanti / Minetest](https://github.com/luanti-org/luanti)<br>luanti-org/luanti | 13,578 | C++ / Lua | 体素沙盒平台和引擎；需另装游戏包 | 仓库核对 |
| [Craft](https://github.com/fogleman/Craft)<br>fogleman/Craft | 11,095 | C / Python | 简化 Minecraft 风格游戏；OpenGL 客户端和 Python 服务端 | 仓库核对 |
| [OpenTTD](https://github.com/OpenTTD/OpenTTD)<br>OpenTTD/OpenTTD | 8,245 | C++ | 运输大亨、铁路物流；可用免费 OpenGFX/OpenSFX 资源 | 仓库核对 |
| [shapez.io](https://github.com/tobspr-games/shapez.io)<br>tobspr-games/shapez.io | 6,958 | JavaScript | 工厂流水线、图形加工；原仓库不再积极维护 | 仓库核对 |
| [The Powder Toy](https://github.com/The-Powder-Toy/The-Powder-Toy)<br>The-Powder-Toy/The-Powder-Toy | 5,294 | C++ / Lua | 落沙物理沙盒；温度、气压与物质反应 | 仓库核对 |
| [CorsixTH](https://github.com/CorsixTH/CorsixTH)<br>CorsixTH/CorsixTH | 4,554 | Lua / C++ | 主题医院重实现；需要原版资源 | 仓库核对 |
| [Widelands](https://github.com/widelands/widelands)<br>widelands/widelands | 3,051 | C++ / Lua | 工人物语风格经营 RTS；生产链与多人模式 | 仓库核对 |
| [A/B Street](https://github.com/a-b-street/abstreet)<br>a-b-street/abstreet | — | Rust | 城市交通仿真与规划软件，兼有模拟交互 | Topics 摘录 |
| [Citybound](https://github.com/citybound/citybound)<br>citybound/citybound | — | Rust | 开发中的多人城市模拟游戏；维护状态未独立复核 | Topics 摘录 |
| [OpenSC2K](https://github.com/nicholas-ochoa/OpenSC2K)<br>nicholas-ochoa/OpenSC2K | — | JavaScript / TypeScript | 模拟城市 2000 重实现；运行完整性未独立复核 | Topics 摘录 |
| [Terasology](https://github.com/MovingBlocks/Terasology)<br>MovingBlocks/Terasology | — | Java | 开源体素世界及模块化游戏项目 | Topics 摘录 |

<a id="category-3"></a>

## RPG、生存、冒险（7 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [Cataclysm-DDA](https://github.com/CleverRaven/Cataclysm-DDA)<br>CleverRaven/Cataclysm-DDA | 13,100 | C++ | 末日回合制生存；程序化世界、物品、制造与战斗 | 仓库核对 |
| [Veloren](https://github.com/veloren/veloren)<br>veloren/veloren | 7,549 | Rust | 体素开放世界动作 RPG，含客户端和服务端；GitHub 为镜像 | 仓库核对 |
| [Endless Sky](https://github.com/endless-sky/endless-sky)<br>endless-sky/endless-sky | 7,549 | C++ | 太空探索、贸易和战斗游戏 | 仓库核对 |
| [Shattered Pixel Dungeon](https://github.com/00-Evan/shattered-pixel-dungeon)<br>00-Evan/shattered-pixel-dungeon | 6,490 | Java | 随机地牢、角色成长、怪物、装备和道具 | 仓库核对 |
| [NetHack](https://github.com/NetHack/NetHack)<br>NetHack/NetHack | 3,899 | C | 经典 Roguelike 地牢冒险 | 仓库核对 |
| [Space Station 14](https://github.com/space-wizards/space-station-14)<br>space-wizards/space-station-14 | 3,782 | C# | 多人空间站角色扮演与模拟 | 仓库核对 |
| [Dungeon Crawl Stone Soup](https://github.com/crawl/crawl)<br>crawl/crawl | 2,987 | C++ | 回合制 Roguelike，职业、技能与地牢战斗 | 仓库核对 |

<a id="category-4"></a>

## MMO 服务端与经典重实现（8 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [openage](https://github.com/SFTtech/openage)<br>SFTtech/openage | 14,425 | C++ / Python | 帝国时代引擎重实现；当前玩法基本不可用，需原版素材 | 仓库核对 |
| [OpenDiablo2](https://github.com/OpenDiablo2/OpenDiablo2)<br>OpenDiablo2/OpenDiablo2 | 11,088 | Go | 暗黑 2 重实现；已归档，需原版及资料片资源 | 仓库核对 |
| [TrinityCore](https://github.com/TrinityCore/TrinityCore)<br>TrinityCore/TrinityCore | 10,756 | C++ | 魔兽世界相关 MMO 服务端框架；不含原版客户端 | 仓库核对 |
| [DevilutionX](https://github.com/diasurgical/DevilutionX)<br>diasurgical/DevilutionX | 9,726 | C++ | 暗黑 1 / Hellfire 移植；需原版或试玩版数据，源码注明非商用 | 仓库核对 |
| [AzerothCore](https://github.com/azerothcore/azerothcore-wotlk)<br>azerothcore/azerothcore-wotlk | 8,885 | C++ | 魔兽世界 3.3.5a 服务端实现；模块化玩法和脚本 | 仓库核对 |
| [OpenMW](https://github.com/OpenMW/openmw)<br>OpenMW/openmw | 6,553 | C++ | 晨风 RPG 引擎重实现；需原版资源，GitHub 为镜像 | 仓库核对 |
| [VCMI](https://github.com/vcmi/vcmi)<br>vcmi/vcmi | 5,843 | C++ | 英雄无敌 3 重实现；需原版游戏资源 | 仓库核对 |
| [Devilution](https://github.com/diasurgical/devilution)<br>diasurgical/devilution | — | C++ | 暗黑 1 还原项目；与 DevilutionX 分开记录 | Topics 摘录 |

<a id="category-5"></a>

## 动作、射击、竞速、音乐（15 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [DOOM](https://github.com/id-Software/DOOM)<br>id-Software/DOOM | 19.5k | C | 经典 FPS 官方源码；需要原版游戏数据 | 仓库核对 |
| [osu!](https://github.com/ppy/osu)<br>ppy/osu | 19.0k | C# | 音乐节奏游戏 lazer 客户端；谱面、编辑器和回放 | 仓库核对 |
| [Quake](https://github.com/id-Software/Quake)<br>id-Software/Quake | 6.0k | C / QuakeC | 经典 FPS 官方源码；需原版或试玩版数据 | 仓库核对 |
| [SuperTuxKart](https://github.com/supertuxkart/stk-code)<br>supertuxkart/stk-code | 5.3k | C++ | 3D 道具卡丁车；需配套 stk-assets 资源仓库 | 仓库核对 |
| [OpenLara](https://github.com/XProger/OpenLara)<br>XProger/OpenLara | 5.1k | C++ | 古墓丽影重实现；不含完整原作素材 | 仓库核对 |
| [Friday Night Funkin'](https://github.com/FunkinCrew/Funkin)<br>FunkinCrew/Funkin | 3,752 | Haxe | 音乐节奏对战；art/assets 是子模块 | 仓库核对 |
| [SuperTux](https://github.com/SuperTux/supertux)<br>SuperTux/supertux | 3.1k | C++ | 马里奥风格横版跳跃，含关卡与资源 | 仓库核对 |
| [Teeworlds](https://github.com/teeworlds/teeworlds)<br>teeworlds/teeworlds | 2.6k | C / C++ | 2D 多人射击；客户端＋服务端 | 仓库核对 |
| [dhewm3](https://github.com/dhewm/dhewm3)<br>dhewm/dhewm3 | 2.2k | C / C++ | DOOM 3 源码移植；需原版非 BFG 数据 | 仓库核对 |
| [Taisei](https://github.com/taisei-project/taisei)<br>taisei-project/taisei | 1.6k | C | 东方题材纵版弹幕，有自己的代码、美术和音乐 | 仓库核对 |
| [AssaultCube](https://github.com/assaultcube/AC)<br>assaultcube/AC | 1.1k | C++ | 多人 FPS；服务器、机器人、录像和地图编辑 | 仓库核对 |
| [Unvanquished](https://github.com/Unvanquished/Unvanquished)<br>Unvanquished/Unvanquished | 1.1k | C++ | FPS＋RTS 游戏逻辑；需另配 Daemon 引擎及资源 | 仓库核对 |
| [DDNet](https://github.com/ddnet/ddnet)<br>ddnet/ddnet | 823 | C++ / C / Rust | 2D 多人合作平台跳跃，含客户端和服务端 | 仓库核对 |
| [Sonic Robo Blast 2](https://github.com/STJr/SRB2)<br>STJr/SRB2 | 560 | C / C++ | 3D 索尼克同人游戏；GitHub 为项目镜像 | 仓库核对 |
| [Neverball](https://github.com/Neverball/neverball)<br>Neverball/neverball | 440 | C | 倾斜地板控制小球，拾取金币和限时闯关 | 仓库核对 |

<a id="category-6"></a>

## 卡牌、棋牌、麻将（13 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [Lichess / lila](https://github.com/lichess-org/lila)<br>lichess-org/lila | 18,713 | Scala / TypeScript | 国际象棋在线平台与服务端 | 仓库核对 |
| [无名杀](https://github.com/libnoname/noname)<br>libnoname/noname | 4,968 | JavaScript | 三国杀类游戏，武将与技能扩展；README 请求勿商用 | 仓库核对 |
| [Forge](https://github.com/Card-Forge/forge)<br>Card-Forge/forge | 2,678 | Java | 万智牌规则、界面、AI、冒险和任务 | 仓库核对 |
| [XMage](https://github.com/magefree/mage)<br>magefree/mage | 2,351 | Java | 万智牌客户端＋服务端，规则裁定和联机 | 仓库核对 |
| [Ratel 旧版](https://github.com/ainilili/ratel)<br>ainilili/ratel | 2.2k | Java / Netty | 命令行斗地主双端；已停止维护 | 仓库核对 |
| [YGOPro](https://github.com/Fluorohydride/ygopro)<br>Fluorohydride/ygopro | 2,055 | C++ / Lua | 游戏王规则和脚本引擎、示例 GUI | 仓库核对 |
| [Cockatrice](https://github.com/Cockatrice/Cockatrice)<br>Cockatrice/Cockatrice | 1,836 | C++ / Qt | 多人联网卡牌虚拟桌面，含客户端和服务端 | 仓库核对 |
| [太阳神三国杀](https://github.com/Mogara/QSanguosha)<br>Mogara/QSanguosha | 1,014 | C++ / Qt / Lua | 经典桌面三国杀源码；构建工具较老 | 仓库核对 |
| [電脳麻将](https://github.com/kobalab/Majiang)<br>kobalab/Majiang | 735 | JavaScript | 日式麻将、AI、牌谱回放；联网另有 majiang-server | 仓库核对 |
| [联网麻将](https://github.com/liumengniu/majiang)<br>liumengniu/majiang | 611 | TypeScript / LayaAir | 麻将客户端；Node.js 服务端另仓库公开 | 仓库核对 |
| [H5 斗地主](https://github.com/svzdev/doudizhu)<br>svzdev/doudizhu | 520 | Python / Phaser | 含客户端、服务端和 SQL | 仓库核对 |
| [Ratel 新版](https://github.com/ratel-online/server)<br>ratel-online/server | 519 | Go | 多人命令行棋牌；麻将存在问题、UNO 开发中 | 仓库核对 |
| [Go 斗地主](https://github.com/dwg255/landlord)<br>dwg255/landlord | 401 | Go / SQLite | 斗地主后端、静态客户端、基础 AI；MIT；客户端源自旧 mailgyc/doudizhu | 仓库核对 |

<a id="category-7"></a>

## 休闲小游戏（5 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [2048](https://github.com/gabrielecirulli/2048)<br>gabrielecirulli/2048 | 13,378 | JavaScript | 2048 网页源码 | 仓库核对 |
| [react-tetris](https://github.com/chvin/react-tetris)<br>chvin/react-tetris | 8,731 | JavaScript / React | 俄罗斯方块、移动端操作、状态保存 | 仓库核对 |
| [Hextris](https://github.com/Hextris/hextris)<br>Hextris/hextris | 2,435 | JavaScript | 六边形消除，网页游戏源码 | 仓库核对 |
| [鱼了个鱼](https://github.com/liyupi/yulegeyu)<br>liyupi/yulegeyu | 1.8k | TypeScript / Vue | 叠层消除，自定义图案和难度 | 仓库核对 |
| [Clumsy Bird](https://github.com/ellisonleao/clumsy-bird)<br>ellisonleao/clumsy-bird | 1,622 | JavaScript | Flappy Bird 风格源码；已归档 | 仓库核对 |

<a id="category-8"></a>

## 捕鱼（3 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [CCFish](https://github.com/fylz1125/CCFish)<br>fylz1125/CCFish | 287 | TypeScript / Cocos Creator | 单机捕鱼达人学习工程，含脚本和场景 | 仓库核对 |
| [Go 捕鱼](https://github.com/dwg255/fish)<br>dwg255/fish | 182 | Go / H5 | 账户、大厅、游戏服务端；完整客户端工程未确认 | 仓库核对 |
| [fishJoy](https://github.com/sherryvs/fishJoy)<br>sherryvs/fishJoy | 49 | JavaScript / Canvas | 小型单机捕鱼；预加载、场景管理、碰撞检测；未见明确 LICENSE | 仓库核对 |

<a id="category-9"></a>

## 编程教育与引擎示例（7 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [WarriorJS](https://github.com/olistic/warriorjs)<br>olistic/warriorjs | 9,537 | TypeScript | 通过编写战斗逻辑闯关，学习 JS/TS | 仓库核对 |
| [Flexbox Froggy](https://github.com/thomaspark/flexboxfroggy)<br>thomaspark/flexboxfroggy | 7,383 | JavaScript | 用闯关方式学习 CSS Flexbox | 仓库核对 |
| [Chop Chop](https://github.com/UnityTechnologies/open-project-1)<br>UnityTechnologies/open-project-1 | 6,091 | C# / Unity | Unity 官方动作冒险示例；2021 年停止开发 | 仓库核对 |
| [Python Games](https://github.com/CharlesPikachu/Games)<br>CharlesPikachu/Games | 5,408 | Python | 多种小游戏的实现合集 | 仓库核对 |
| [ActionRoguelike](https://github.com/tomlooman/ActionRoguelike)<br>tomlooman/ActionRoguelike | 4,582 | C++ / Unreal Engine | UE5 合作动作学习项目；主分支含实验系统 | 仓库核对 |
| [You Don't Need JavaScript](https://github.com/you-dont-need/You-Dont-Need-JavaScript)<br>you-dont-need/You-Dont-Need-JavaScript | — | HTML / CSS | CSS 能力和交互示例合集，含 game 标签 | Topics 摘录 |
| [Server Survival](https://github.com/pshenok/server-survival)<br>pshenok/server-survival | — | JavaScript / Three.js | 通过塔防学习云架构、流量与扩容 | Topics 摘录 |

<a id="category-10"></a>

## 游戏引擎与客户端框架（15 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [PixiJS](https://github.com/pixijs/pixijs)<br>pixijs/pixijs | — | TypeScript | 2D WebGL 渲染库 | Topics 摘录 |
| [GDevelop](https://github.com/4ian/GDevelop)<br>4ian/GDevelop | — | JavaScript | 跨平台 2D/3D 游戏引擎与编辑器 | Topics 摘录 |
| [libGDX](https://github.com/libgdx/libgdx)<br>libgdx/libgdx | — | Java | 桌面和移动端游戏开发框架 | Topics 摘录 |
| [Pyxel](https://github.com/kitao/pyxel)<br>kitao/pyxel | — | Rust / Python API | 复古像素游戏引擎 | Topics 摘录 |
| [Ebitengine](https://github.com/hajimehoshi/ebiten)<br>hajimehoshi/ebiten | — | Go | 2D 游戏引擎 | Topics 摘录 |
| [Flame](https://github.com/flame-engine/flame)<br>flame-engine/flame | — | Dart / Flutter | Flutter 游戏引擎 | Topics 摘录 |
| [WebGAL](https://github.com/OpenWebGAL/WebGAL)<br>OpenWebGAL/WebGAL | — | TypeScript | 网页端视觉小说引擎 | Topics 摘录 |
| [Dialogic](https://github.com/dialogic-godot/dialogic)<br>dialogic-godot/dialogic | — | GDScript | Godot 对话、角色与视觉小说插件 | Topics 摘录 |
| [Hilo](https://github.com/hiloteam/Hilo)<br>hiloteam/Hilo | — | JavaScript | 跨端 HTML5 游戏开发方案 | Topics 摘录 |
| [Magnum](https://github.com/mosra/magnum)<br>mosra/magnum | — | C++ | 模块化图形中间件，面向游戏与可视化 | Topics 摘录 |
| [FXGL](https://github.com/AlmasB/FXGL)<br>AlmasB/FXGL | — | Java / Kotlin / JavaFX | 2D/3D 游戏库和引擎 | Topics 摘录 |
| [Urho3D](https://github.com/urho3d/urho3d)<br>urho3d/urho3d | — | C++ | 游戏引擎 | Topics 摘录 |
| [ggez](https://github.com/ggez/ggez)<br>ggez/ggez | — | Rust | 2D 游戏开发库 | Topics 摘录 |
| [react-game-kit](https://github.com/FormidableLabs/react-game-kit)<br>FormidableLabs/react-game-kit | — | JavaScript / React | React 与 React Native 游戏组件库 | Topics 摘录 |
| [boardgame.io](https://github.com/boardgameio/boardgame.io)<br>boardgameio/boardgame.io | — | TypeScript / JavaScript | 回合制游戏状态管理与多人联网框架 | 仓库核对 |

<a id="category-11"></a>

## 网络、ECS与开发工具（12 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [ET](https://github.com/egametang/ET)<br>egametang/ET | — | C# / Unity | Unity 客户端和 C# 服务端框架 | Topics 摘录 |
| [Nakama](https://github.com/heroiclabs/nakama)<br>heroiclabs/nakama | — | Go | 多人游戏后端，匹配、排行榜、聊天和社交 | Topics 摘录 |
| [Tiled](https://github.com/mapeditor/tiled)<br>mapeditor/tiled | — | C++ / Qt | 地图与关卡编辑器 | Topics 摘录 |
| [Luban](https://github.com/focus-creative-games/luban)<br>focus-creative-games/luban | — | C# | 游戏配置与数据表工具 | Topics 摘录 |
| [Recast Navigation](https://github.com/recastnavigation/recastnavigation)<br>recastnavigation/recastnavigation | — | C++ | 导航网格、寻路与人群模拟工具集 | Topics 摘录 |
| [Airtest](https://github.com/AirtestProject/Airtest)<br>AirtestProject/Airtest | — | Python | 游戏和应用的 UI 自动化测试框架 | Topics 摘录 |
| [Entitas](https://github.com/sschmid/Entitas)<br>sschmid/Entitas | — | C# | 面向 C# 和 Unity 的 ECS 框架 | Topics 摘录 |
| [tModLoader](https://github.com/tModLoader/tModLoader)<br>tModLoader/tModLoader | — | C# | Terraria 模组开发与加载工具，依赖 Terraria 安装 | Topics 摘录 |
| [cute_headers](https://github.com/RandyGaul/cute_headers)<br>RandyGaul/cute_headers | — | C / C++ | 游戏常用单头文件库，覆盖音频、图形、碰撞等 | Topics 摘录 |
| [NoahGameFrame](https://github.com/ketoo/NoahGameFrame)<br>ketoo/NoahGameFrame | — | C++ | 分布式游戏服务端框架、Actor 与网络库 | Topics 摘录 |
| [Godot Shaders](https://github.com/gdquest-demos/godot-shaders)<br>gdquest-demos/godot-shaders | — | GDShader | Godot Shader 素材和可玩演示 | Topics 摘录 |
| [cellnet](https://github.com/davyxu/cellnet)<br>davyxu/cellnet | — | Go | 网络通信库，支持游戏网络开发 | Topics 摘录 |

<a id="category-12"></a>

## 资料导航与资源合集（9 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [Games on GitHub](https://github.com/leereilly/games)<br>leereilly/games | 24,947 | Markdown | 按类型收集游戏、插件、地图等；已归档 | 仓库核对 |
| [Open Source iOS Apps](https://github.com/dkhamsing/open-source-ios-apps)<br>dkhamsing/open-source-ios-apps | — | Swift / Objective-C 等 | 开源 iOS 应用合集，包含游戏；并非纯游戏目录 | Topics 摘录 |
| [Awesome Python Applications](https://github.com/mahmoud/awesome-python-applications)<br>mahmoud/awesome-python-applications | — | Python 生态 | Python 应用合集，覆盖游戏和其他软件 | Topics 摘录 |
| [Chinese DOS Games](https://github.com/rwv/chinese-dos-games)<br>rwv/chinese-dos-games | — | Python / DOS 游戏资源 | 中文 DOS 游戏合集；不是这些游戏的源码合集 | Topics 摘录 |
| [Multiplayer Networking Resources](https://github.com/0xFA11/MultiplayerNetworkingResources)<br>0xFA11/MultiplayerNetworkingResources | — | 多语言资料 | 多人游戏网络编程资料索引 | Topics 摘录 |
| [Open Source Flash](https://github.com/open-source-flash/open-source-flash)<br>open-source-flash/open-source-flash | — | ActionScript 相关 | 呼吁开放 Flash/Shockwave 规范的倡议项目，并非游戏源码 | Topics 摘录 |
| [GameDev Resources](https://github.com/Kavex/GameDev-Resources)<br>Kavex/GameDev-Resources | — | 多语言资料 | 游戏开发资源导航 | Topics 摘录 |
| [GameDevMind](https://github.com/gonglei007/GameDevMind)<br>gonglei007/GameDevMind | — | 多语言资料 | 游戏开发技术图谱和知识导航 | Topics 摘录 |
| [Anything About Game](https://github.com/killop/anything_about_game)<br>killop/anything_about_game | — | 多语言资料 | 游戏开发资源与工具合集 | Topics 摘录 |

<a id="category-13"></a>

## 配套客户端、服务端与资源（5 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [電脳麻将服务端](https://github.com/kobalab/majiang-server)<br>kobalab/majiang-server | — | JavaScript / Node.js | 配套 kobalab/Majiang 的联网服务端 | 仓库核对 |
| [联网麻将服务端](https://github.com/liumengniu/majiang-server)<br>liumengniu/majiang-server | 134 | JavaScript / Node.js | 配套湖南麻将服务端 | 仓库核对 |
| [Ratel 客户端](https://github.com/ratel-online/client)<br>ratel-online/client | — | 未记录 | Ratel 新版配套客户端 | 仓库核对 |
| [shapez Community Edition](https://github.com/shapez-community/shapez-community-edition)<br>shapez-community/shapez-community-edition | — | 未记录 | shapez 原 README 推荐的社区维护方向 | 配套引用 |
| [osu! resources](https://github.com/ppy/osu-resources)<br>ppy/osu-resources | — | 游戏资源 | osu! 配套资源，许可与客户端代码分别规定 | 配套引用 |

<a id="category-14"></a>

## 算法、AI与规则库（4 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [DouZero](https://github.com/kwai/DouZero)<br>kwai/DouZero | 4.7k | Python | 斗地主强化学习、训练和评估框架；Apache-2.0 | 仓库核对 |
| [q_algorithm](https://github.com/yuanfengyun/q_algorithm)<br>yuanfengyun/q_algorithm | 2.1k | Lua / C++ / C# / Go / JS / Java / Python | 麻将、跑胡子、扑克牌型与胡牌算法；未见明确 LICENSE | 仓库核对 |
| [DeepLearningFlappyBird](https://github.com/yenchenlin/DeepLearningFlappyBird)<br>yenchenlin/DeepLearningFlappyBird | — | Python | Flappy Bird 深度强化学习示例 | Topics 摘录 |
| [Solvitaire](https://github.com/thecharlieblake/Solvitaire)<br>thecharlieblake/Solvitaire | 51 | C++ | 多种单人纸牌游戏的求解器；GPL-2.0 | 低星搜索核对 |

<a id="category-15"></a>

## 备选与范围受限项目（12 个）

这些项目保留用于查找和排除记录。销售推广、部署包、缺少服务端或授权说明冲突等情况已列在备注中，收录不等于推荐使用。

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [libccy/noname](https://github.com/libccy/noname)<br>libccy/noname | 181 | 未记录 | 与 libnoname/noname 分开保留；此前核对热度较低，不混用 Star | 仓库核对 |
| [openinggame/qp](https://github.com/openinggame/qp)<br>openinggame/qp | 427 | 部署文件 | 部署包、截图和数据库；依赖预构建 Docker 镜像，未确认为完整源码 | 仓库核对 |
| [eenot 棋牌客户端](https://github.com/zhpelo/eenot_qpgame_client)<br>zhpelo/eenot_qpgame_client | 73 | JavaScript / Cocos2d-js | 配套捕鱼、斗地主、牛牛等；来自第三方源码网站，完整性未验证 | 仓库核对 |
| [eenot 棋牌服务端](https://github.com/zhpelo/eenot_qpgame_server)<br>zhpelo/eenot_qpgame_server | 108 | Node.js | 综合棋牌业务与 SQL；标 Artistic-2.0，存在推广内容，作为次选 | 仓库核对 |
| [ChessCard](https://github.com/MakeHui/ChessCard)<br>MakeHui/ChessCard | 25 | 未记录 | README 明确服务端无法开源，不能据此直接运行完整游戏 | 候选核对 |
| [Beimi](https://github.com/calanay/beimi)<br>calanay/beimi | 158 | Java / Cocos Creator | 综合棋牌，Apache-2.0；美术资源迁移，完整性待查 | 候选核对 |
| [card-games](https://github.com/huff7/card-games)<br>huff7/card-games | 8 | 未记录 | README 要求私聊价格，属于销售推广候选 | 候选核对 |
| [OpenMajiang fork](https://github.com/coood/OpenMajiang)<br>coood/OpenMajiang | 2 | 未记录 | 低星 fork，不能替代已失效原仓库的历史热度 | 候选核对 |
| [msqp](https://github.com/mszlu521/msqp)<br>mszlu521/msqp | 81 | Go / MongoDB / Redis | 棋牌服务端教程；GitHub 标 Apache-2.0，但 README 对未购课商用有限制 | 候选核对 |
| [fishing-master-arcade](https://github.com/masterai-top/fishing-master-arcade)<br>masterai-top/fishing-master-arcade | 6 | 未记录 | 要求联系购买完整捕鱼源码的推广候选 | 候选核对 |
| [Fishing-Game-Complete-Source-Art](https://github.com/alibabama401/Fishing-Game-Complete-Source-Art)<br>alibabama401/Fishing-Game-Complete-Source-Art | 6 | 未记录 | 要求联系购买完整捕鱼源码，未认定为完整公开工程 | 候选核对 |
| [Fishing-Game-Source-Code](https://github.com/niubideren111/Fishing-Game-Source-Code)<br>niubideren111/Fishing-Game-Source-Code | 10 | 未记录 | 捕鱼源码销售推广候选，完整性未验证 | 候选核对 |

<a id="category-16"></a>

## 运行环境、模拟器与运维工具（5 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [Whisky](https://github.com/Whisky-App/Whisky)<br>Whisky-App/Whisky | — | Swift | macOS Wine 包装工具，辅助运行游戏 | Topics 摘录 |
| [CompactGUI](https://github.com/IridiumIO/CompactGUI)<br>IridiumIO/CompactGUI | — | VB.NET / Windows | 通过 Windows 原生压缩能力减少游戏和程序占用 | Topics 摘录 |
| [Provenance](https://github.com/Provenance-Emu/Provenance)<br>Provenance-Emu/Provenance | — | C / Swift / Objective-C | iOS/tvOS 多平台模拟器前端 | Topics 摘录 |
| [LinuxGSM](https://github.com/GameServerManagers/LinuxGSM)<br>GameServerManagers/LinuxGSM | — | Shell | Linux 专用游戏服务器部署和管理工具 | Topics 摘录 |
| [gameboy.live](https://github.com/HFO4/gameboy.live)<br>HFO4/gameboy.live | — | Go | Game Boy 模拟器，含终端云游戏功能 | Topics 摘录 |

<a id="category-17"></a>

## 关联但非游戏项目（1 个）

保留原 Topics 摘录中带有 game 标签、但主要用途不是游戏开发的项目，避免把标签等同于内容类型。

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [pikapika](https://github.com/ComicSparks/pikapika)<br>ComicSparks/pikapika | — | Dart / Flutter | 跨平台漫画阅读器，因 game 标签出现在原 Topics 列表 | Topics 摘录 |

<a id="category-18"></a>

## 低星纸牌与消除示例（10 个）

| 项目／仓库 | Star | 语言／技术 | 内容、源码范围与限制 | 信息依据 |
|---|---:|---|---|---|
| [Aisleriot](https://github.com/GNOME/aisleriot)<br>GNOME/aisleriot | 42 | 未记录 | 80 多种单人纸牌玩法；GitHub 是 GNOME GitLab 只读镜像 | 低星搜索核对 |
| [fSolitaire](https://github.com/fuzzley/fSolitaire)<br>fuzzley/fSolitaire | 1 | TypeScript / Angular / Phaser | FreeCell、Baker's Game 等纸牌游戏；代码 GPL-3.0，卡图另有许可 | 低星搜索核对 |
| [Solitaire Game](https://github.com/Tynab/Solitaire-Game)<br>Tynab/Solitaire-Game | 9 | C# / Unity | Unity 单人纸牌示例，有 WebGL 演示 | 低星搜索核对 |
| [Single-file Solitaire](https://github.com/jhatzimalis/solitaire)<br>jhatzimalis/solitaire | 1 | HTML / CSS / JavaScript | 单文件 Klondike 纸牌游戏，离线运行；MIT | 低星搜索核对 |
| [Qt Solitaire](https://github.com/iUltimateLP/solitaire)<br>iUltimateLP/solitaire | 4 | C++ / Qt | Klondike 纸牌课程项目；MIT | 低星搜索核对 |
| [Hitster](https://github.com/Timtam/hitster)<br>Timtam/hitster | 55 | 未记录 | Hitster 卡牌游戏的非官方网页实现；GPL-3.0 | 低星搜索核对 |
| [Cards](https://github.com/mjlomeli/cards)<br>mjlomeli/cards | 0 | JavaScript / HTML5 | 单人纸牌概念验证与学习项目；README 声明 MIT | 低星搜索核对 |
| [Solitaire](https://github.com/HectorVilas/solitaire)<br>HectorVilas/solitaire | 4 | JavaScript / HTML / CSS | 经典 Klondike 网页纸牌实现 | 低星搜索核对 |
| [Mah](https://github.com/ffalt/mah)<br>ffalt/mah | 139 | HTML5 | 麻将牌消除（Mahjong Solitaire），与四人麻将不同；MIT | 低星搜索核对 |
| [Klondike](https://github.com/scottwillmoore/klondike)<br>scottwillmoore/klondike | 1 | Rust / TypeScript | Klondike 单人纸牌实现；MIT | 低星搜索核对 |

## 已核对项目的补充说明

### 卡牌与棋牌授权摘要

| 仓库 | 已记录的授权说明 |
|---|---|
| [libnoname/noname](https://github.com/libnoname/noname) | GPL-3.0；README 另请求保留出处、不要用于商业用途。 |
| [Card-Forge/forge](https://github.com/Card-Forge/forge) | GPL-3.0。 |
| [magefree/mage](https://github.com/magefree/mage) | MIT。 |
| [Fluorohydride/ygopro](https://github.com/Fluorohydride/ygopro) | 主仓库 GPL-2.0；不能将核心子仓库的 MIT 当作整个 GUI 项目的许可。 |
| [Cockatrice/Cockatrice](https://github.com/Cockatrice/Cockatrice) | GPL-2.0。 |
| [Mogara/QSanguosha](https://github.com/Mogara/QSanguosha) | 代码 GPL-3.0，素材 CC BY-NC-ND 4.0；旧构建说明使用较老的 VS/Qt。 |
| [ainilili/ratel](https://github.com/ainilili/ratel) | Apache-2.0；旧版停止维护。 |
| [ratel-online/server](https://github.com/ratel-online/server) | MIT；README 明确麻将存在问题、UNO 开发中。 |
| [liumengniu/majiang](https://github.com/liumengniu/majiang)、[kobalab/Majiang](https://github.com/kobalab/Majiang) | 首页标 MIT；联网服务分别使用各自配套服务器。 |
| [svzdev/doudizhu](https://github.com/svzdev/doudizhu) | 此前查看首页与文件树未见明确 LICENSE；文档部分地址仍用历史用户名。 |

### 捕鱼及前端完整性

- [CCFish](https://github.com/fylz1125/CCFish) 基于 Cocos Creator 2.2.2，包含脚本、场景、贴图、动画、音效与预制体。作者说明尚未完全完善；首页未见明确 LICENSE。
- [dwg255/fish](https://github.com/dwg255/fish) 公开 account、hall、game 服务端源码；client 目录是图片和压缩包。[Issue #3](https://github.com/dwg255/fish/issues/3) 有前端为编译产物的反馈，因此完整客户端工程未确认。README 声明 MIT，但根目录未见单独 LICENSE。
- [openinggame/qp](https://github.com/openinggame/qp) 主要公开部署文件、截图和数据库压缩包，依赖预构建镜像；不应据此认定已经拿到完整游戏源码。

### 资源、镜像和维护状态

- [VCMI 安装说明](https://github.com/vcmi/vcmi/blob/develop/docs/players/Installation_Windows.md) 明确需要 Heroes III 原版数据。
- [OpenRA FAQ](https://github.com/OpenRA/OpenRA/wiki/FAQ) 说明首次下载或导入游戏素材，不要求预装原版；原游戏素材与引擎代码许可不同。
- [SuperTuxKart 安装说明](https://github.com/supertuxkart/stk-code/blob/master/INSTALL.md) 要求获取代码与独立的 stk-assets 资源。
- [DevilutionX README](https://github.com/diasurgical/DevilutionX) 说明原版数据与 shareware 数据的使用范围，并注明源码非商用。
- [OpenMW README](https://github.com/OpenMW/openmw) 说明运行 Morrowind 需要拥有原游戏，GitHub 是主开发仓库的镜像。
- [openage README](https://github.com/SFTtech/openage) 明确当前 gameplay 基本不可用，不适合按完整成品游戏评估。
- [Chop Chop README](https://github.com/UnityTechnologies/open-project-1) 明确自 2021 年 12 月起停止开发。
- [ActionRoguelike README](https://github.com/tomlooman/ActionRoguelike) 说明主分支含实验系统，可能影响稳定性和联机支持；部分游戏素材仅许可用于 Unreal Engine。
- [shapez README](https://github.com/tobspr-games/shapez.io) 推荐另列的 Community Edition 作为后续维护方向。

## 旧地址与失败链接记录

这些地址没有作为新的有效项目计入仓库总数：

- `zuoge85/OpenMajiang`：此前打开返回 404；另将找到的低星 fork `coood/OpenMajiang` 单独记录，不沿用原仓库热度。
- `shattered-pixel/shattered-pixel-dungeon`：未能打开；已核对并采用 `00-Evan/shattered-pixel-dungeon`。
- `lfz/battle-city`：此前打开返回 404，没有列入推荐条目。
- `ChristerNilsson/2048`：此前未能获取页面，没有据此记录 Star 或作推荐。

## 阅读建议

- 看完整多人游戏和自动化系统：Mindustry。
- 看开放世界客户端、服务端与世界生成：Veloren。
- 看经营模拟：OpenTTD；看回合策略：Unciv、Wesnoth。
- 看地牢、装备和角色成长：Shattered Pixel Dungeon。
- 看复杂卡牌规则：无名杀、Forge、XMage、YGOPro。
- 看网络棋牌：liumengniu/majiang 与配套服务端、svzdev/doudizhu、Ratel 新版。
- 看捕鱼客户端：CCFish；看捕鱼服务端：dwg255/fish。
- 看 Unity/Unreal 示例：Chop Chop、ActionRoguelike，注意对应维护状态和素材要求。
- 看小型网页游戏：2048、Hextris、react-tetris。

本目录仅保存公开项目与已记录说明；没有下载或编译这些游戏，没有执行运行验收。源码许可、第三方素材许可和原版数据要求应按选定仓库的实际文件分别核对。
