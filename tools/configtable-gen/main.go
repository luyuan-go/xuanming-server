// configtable-gen 配置表生成器入口(描述符驱动,做法对齐旧项目 protogen)。
//
// 用法(仓库根目录):
//
//	go run ./tools/configtable-gen -tables F:\work\Pandora-Client-SVN\Table -source-rev svn-r123 [-out configtable/dist] [-go-out pkg/configtable] [-version N] [-bitindex-bootstrap]
//
// -source-rev 必填(产物 manifest 溯源);-bitindex-bootstrap 仅限 bit_index 表从未发布过时
// 显式初始化状态文件(已发布表丢状态属事故,先从 git 恢复,禁止静默重分配位序)。
//
// 表清单零手写登记:pandora.config.v1 包内打了 (excel_file) 注解的 <Name>TableData
// 容器即一张配置表(见 excel.proto),生成器经 protoreflect 自动发现。一次产出:
//  1. 数据:xlsx → excel 注解驱动的通用构建器(§7 严格校验,失败整批不产出)
//     → dist/*.json + manifest.json(version 单调 + sha256;内容不变幂等跳过);
//  2. 代码:pkg/configtable 的 <name>_table.gen.go + tables.gen.go(内容不变不重写),
//     伴生文件 <name>.go 缺失时创建一次空钩子桩(此后归人维护,不覆盖)。
//
// 加新表:写 proto(带 excel 注解)→ pwsh tools/scripts/proto_gen.ps1 → 跑本工具。
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/tools/configtable-gen/internal/gogen"
	"github.com/luyuancpp/pandora/tools/configtable-gen/internal/tablegen"
	"github.com/luyuancpp/pandora/tools/configtable-gen/internal/xlsxlite"
)

func main() {
	tablesRoot := flag.String("tables", "", "源表根目录(必填,如 F:\\work\\Pandora-Client-SVN\\Table)")
	outDir := flag.String("out", filepath.Join("configtable", "dist"), "数据产物输出目录")
	goOut := flag.String("go-out", filepath.Join("pkg", "configtable"), "Go 表代码输出目录(空 = 跳过代码生成)")
	bitStateDir := flag.String("bitindex-state", filepath.Join("configtable", "bitindex_state"),
		"位序状态目录((excel_bit_index) 表;git 跟踪,丢失会导致已落库位图错位)")
	bitBootstrap := flag.String("bitindex-bootstrap", "",
		"允许**指定表**(逗号分隔表名)在位序状态文件缺失时从零初始化(审计 P1:全局布尔会让"+
			"任何意外丢状态的旧表被静默重初始化、复用历史 bit 错误解释已落库位图;仅限从未发布过的新表)")
	sourceRev := flag.String("source-rev", "",
		"源表版本标注(必填,如 svn-r123;产物 manifest 溯源依据,不允许不可追溯批次)")
	forceVersion := flag.Uint64("version", 0, "强制指定版本号(默认自动单调递增)")
	syncMode := flag.Bool("sync", false,
		"表头漂移同步:比对 xlsx 表头与 proto (excel_col) 注解并报告差异,不改任何文件也不产出")
	syncWrite := flag.Bool("sync-write", false,
		"随 -sync 使用:把「列改名」「末尾加列」这类机械漂移写回 .proto(删列 / 挪位只报告)")
	protoRoot := flag.String("proto-root", "proto", "-sync-write 改写 .proto 的根目录")
	clientRegistry := flag.String("client-registry", "",
		"客户端列登记目录(默认 <-tables>/../Tool/Table/Cs/Proto;新增列的字段名 / 类型取这里,保证两仓一致)")
	var syncCols stringList
	flag.Var(&syncCols, "sync-col",
		"指定新增列的字段名 / 类型,格式 <表名>.<列名>=<字段名>[:<类型>],可重复")
	flag.Parse()

	if *tablesRoot == "" {
		fmt.Fprintln(os.Stderr, "缺少 -tables 源表根目录")
		flag.Usage()
		os.Exit(2)
	}

	// 同步模式只读 xlsx 表头、只写 .proto,不碰 dist / Go / 位序状态,
	// 因此走在生成锁之前并直接返回(不与生成流程共用锁,也不需要 -source-rev)。
	if *syncMode || *syncWrite {
		os.Exit(runSync(syncOptions{
			tablesRoot: *tablesRoot,
			protoRoot:  *protoRoot,
			registry:   *clientRegistry,
			write:      *syncWrite,
			overrides:  syncCols,
		}))
	}
	// source_rev 溯源门禁(审计 P1):空白与 "unknown" 占位一律拒(只拒空字符串时,
	// unknown/纯空白照样产出不可追溯批次)。
	*sourceRev = strings.TrimSpace(*sourceRev)
	if *sourceRev == "" || strings.EqualFold(*sourceRev, "unknown") {
		fmt.Fprintln(os.Stderr, "缺少有效 -source-rev 源表版本标注(如 svn-r123;不接受空白/unknown 占位,发布产物必须可追溯到源表版本)")
		flag.Usage()
		os.Exit(2)
	}

	bootstrapTables := map[string]bool{}
	for _, name := range strings.Split(*bitBootstrap, ",") {
		if name = strings.TrimSpace(name); name != "" {
			bootstrapTables[name] = true
		}
	}

	// 生成排他锁(审计 P1:两个生成进程并发会各自读同版本产不同批次,交错写
	// Go/dist/位序状态互相覆盖)。锁必须覆盖**全部写入目录**(审计 R4 #12:只锁 dist
	// 时,不同 -out 同 -go-out 的两个进程仍会交错写 Go/位序状态);超过 10 分钟视为
	// 陈旧崩溃残留,夺取走原子改名(防两个进程同时判旧、互删对方新锁的 ABA)。
	lockDirs := []string{*outDir, *bitStateDir}
	if *goOut != "" {
		lockDirs = append(lockDirs, *goOut)
	}
	unlock, lerr := acquireGenLock(lockDirs...)
	if lerr != nil {
		fatalf("%v", lerr)
	}
	// fatalf 走 os.Exit 不跑 defer(审计 R4 #12 exit 泄漏:任意校验失败都会把锁留满
	// 10 分钟),注册到 fatalf 前置清理;defer 兜底正常返回路径,token 校验保证幂等。
	exitCleanup = unlock
	defer unlock()

	defs, err := tablegen.Discover()
	if err != nil {
		fatalf("发现配置表失败: %v", err)
	}

	// 1. 数据产物:xlsx → 通用构建器(excel 注解驱动)→ dist
	var generated []tablegen.Generated
	built := make(map[string]proto.Message, len(defs))
	for i := range defs {
		def := &defs[i]
		path := filepath.Join(*tablesRoot, filepath.FromSlash(def.ExcelFile))
		sheet, err := xlsxlite.ReadFirstSheet(path)
		if err != nil {
			fatalf("读 %s 失败: %v", path, err)
		}
		data, rows, err := def.Build(sheet.Rows)
		if err != nil {
			fatalf("校验失败,整批不产出:\n  %v", err)
		}
		generated = append(generated, tablegen.Generated{
			Name: def.Name, ProtoName: def.ProtoName, Data: data, RowCount: rows,
		})
		built[def.Name] = data
		fmt.Printf("[OK] %-8s %3d 行  ← %s\n", def.Name, rows, def.ExcelFile)
	}

	// 1.5 批内引用完整性((excel_fk),§7.5):全表构建完成后统一校验,失败整批不产出。
	if err := tablegen.ValidateFKs(defs, built); err != nil {
		fatalf("外键校验失败,整批不产出:\n  %v", err)
	}

	// 1.6 位序门禁((excel_bit_index) 表):必须在写 dist **之前**通过,且不因
	// -go-out "" 跳过(审计 P1:门禁放在写 dist 之后,失败时磁盘上已留下可发布的
	// 新批次)。本步只在内存中 Load/Assign/生成映射,**不落任何盘**;位序状态文件
	// 在数据产物与 Go 映射都写成后最后保存(审计 P1:状态先落盘会造成"JSON 状态已
	// 推进、编译期 Go map 未更新"的可发布劈叉批次;Assign 对同一状态 + 同一 ID 序列
	// 是确定性的,崩溃重跑得到相同分配,状态最后写不破坏稳定性)。
	type bitState struct {
		def       string
		statePath string
		state     *tablegen.BitState
		changed   bool
	}
	bitFiles := make(map[string][]byte)
	var bitStates []bitState
	for _, def := range defs {
		if !def.BitIndex {
			continue
		}
		if *goOut == "" {
			// 位序表的 JSON 位图与编译期 Go map 必须同批演进:不产 Go 映射的批次
			// 一旦发布,新增 bit ID 会在运行侧错位(审计 P1)。
			fatalf("表 %s 带 (excel_bit_index):必须同时指定 -go-out(位序 JSON 与编译期 Go 映射必须同批产出)", def.Name)
		}
		statePath := filepath.Join(*bitStateDir, def.Name+".json")
		state, err := tablegen.LoadBitState(statePath, bootstrapTables[def.Name])
		if err != nil {
			fatalf("表 %s: %v", def.Name, err)
		}
		live, changed := state.Assign(def.RowIDs(built[def.Name]))
		raw, err := gogen.BitIndexFile(def, live, state.BitCount())
		if err != nil {
			fatalf("表 %s 生成位序映射失败: %v", def.Name, err)
		}
		bitFiles[def.Name+"_bitindex.gen.go"] = raw
		bitStates = append(bitStates, bitState{def: def.Name, statePath: statePath, state: state, changed: changed})
	}

	// 2. Go 表代码**先于 dist 落盘**(审计 P1 失败原子方向:先写可发布的 dist 再写
	// Go,后半段失败会留下"新 JSON 配旧二进制"的劈叉批次;反过来"新 Go 配旧 JSON"
	// 是安全方向——新增位/常量未被旧数据引用,旧服务行为不变)。
	// -go-out "" 只允许在**没有位序表**时跳过(1.6 已强制校验)。
	if *goOut != "" {
		files, gerr := gogen.Files(defs)
		if gerr != nil {
			fatalf("%v", gerr)
		}
		for name, raw := range bitFiles {
			files[name] = raw
		}
		changed, err := writeGeneratedFiles(*goOut, files)
		if err != nil {
			fatalf("写 Go 表代码失败: %v", err)
		}
		for _, name := range changed {
			fmt.Printf("[GEN] %s\n", filepath.Join(*goOut, name))
		}
		if len(changed) == 0 {
			fmt.Printf("Go 表代码无变化(%s,%d 个文件)\n", *goOut, len(files))
		}
	}

	// 2.5 数据产物(dist):Go 映射就绪后才产出可发布批次。
	res, err := tablegen.WriteBatch(generated, tablegen.Options{
		OutDir:       *outDir,
		SourceRev:    *sourceRev,
		ForceVersion: *forceVersion,
		Now:          time.Now(),
	})
	if err != nil {
		fatalf("写数据产物失败: %v", err)
	}
	switch {
	case res.Unchanged && res.SourceRevCorrected:
		fmt.Printf("数据与上一批一致,保持 version %d;manifest source_rev 已原地纠正为 %s\n", res.Version, *sourceRev)
	case res.Unchanged:
		fmt.Printf("数据与上一批一致,保持 version %d,未写盘\n", res.Version)
	default:
		fmt.Printf("数据批次 version %d → %s(%d 张表)\n", res.Version, *outDir, len(res.Tables))
	}

	// 2.6 位序状态文件最后落盘(原子 rename;1.6 注释:dist 与 Go 映射都写成后才推进
	// 状态;此前任何一步失败,状态保持原样,重跑得到相同分配,零劈叉残留)。
	for _, bs := range bitStates {
		if !bs.changed {
			continue
		}
		if err := tablegen.SaveBitState(bs.statePath, bs.state); err != nil {
			fatalf("表 %s 写位序状态失败: %v", bs.def, err)
		}
		fmt.Printf("[BIT] %s 位序状态更新 → %s\n", bs.def, bs.statePath)
	}

	if *goOut == "" {
		return
	}

	// 3. 伴生桩:<name>.go 缺失时创建一次(空 validate 钩子),此后归人维护不覆盖
	for _, def := range defs {
		path := filepath.Join(*goOut, def.Name+".go")
		if _, err := os.Stat(path); err == nil {
			continue
		}
		stub, err := gogen.CompanionStub(def)
		if err != nil {
			fatalf("生成 %s 伴生桩失败: %v", def.Name, err)
		}
		if err := os.WriteFile(path, stub, 0o644); err != nil {
			fatalf("写 %s 失败: %v", path, err)
		}
		fmt.Printf("[NEW] %s(伴生桩,补业务校验 / 域方法后归人维护)\n", path)
	}
}

// genLockStaleAfter 之后的锁视为崩溃残留(生成通常秒级完成)。
const genLockStaleAfter = 10 * time.Minute

// acquireGenLock 在每个写入目录下创建排他锁文件(O_EXCL,按路径排序定序防死锁),
// 返回释放全部锁的函数。任一目录取锁失败即回滚已取的锁。
func acquireGenLock(dirs ...string) (func(), error) {
	uniq := map[string]bool{}
	var sorted []string
	for _, d := range dirs {
		abs, err := filepath.Abs(d)
		if err != nil {
			return nil, fmt.Errorf("解析锁目录 %s: %w", d, err)
		}
		if !uniq[abs] {
			uniq[abs] = true
			sorted = append(sorted, abs)
		}
	}
	slices.Sort(sorted)

	var unlocks []func()
	unlockAll := func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}
	for _, dir := range sorted {
		unlock, err := acquireDirLock(dir)
		if err != nil {
			unlockAll()
			return nil, err
		}
		unlocks = append(unlocks, unlock)
	}
	return unlockAll, nil
}

// acquireDirLock 单目录取锁。陈旧锁夺取用**原子改名**(只有一个夺取者改名成功),
// 不做无条件删除(审计 R4 #12 ABA:两个进程同时判旧、A 删旧建新、B 再删的是 A 的
// 新锁 → 双持有)。释放时校验锁内容仍是本进程 token 才删(锁被夺取后不误删新主锁;
// read-then-remove 的微小窗口只在本进程停顿超 10 分钟被夺取时才可达,风险量级可接受)。
func acquireDirLock(dir string) (func(), error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("创建锁目录 %s: %w", dir, err)
	}
	lockPath := filepath.Join(dir, ".gen.lock")
	token := fmt.Sprintf("pid=%d nonce=%d started=%s\n", os.Getpid(), time.Now().UnixNano(), time.Now().Format(time.RFC3339))
	for attempt := 0; attempt < 3; attempt++ {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			_, werr := f.WriteString(token)
			cerr := f.Close()
			if werr != nil || cerr != nil {
				_ = os.Remove(lockPath)
				return nil, fmt.Errorf("写生成锁 %s 失败: %v/%v", lockPath, werr, cerr)
			}
			return func() {
				if data, rerr := os.ReadFile(lockPath); rerr == nil && string(data) == token {
					_ = os.Remove(lockPath)
				}
			}, nil
		}
		if !os.IsExist(err) {
			return nil, fmt.Errorf("创建生成锁 %s: %w", lockPath, err)
		}
		info, serr := os.Stat(lockPath)
		if serr != nil {
			continue // 锁刚被释放/夺取,重新尝试创建
		}
		if time.Since(info.ModTime()) <= genLockStaleAfter {
			return nil, fmt.Errorf("另一个 configtable-gen 正在运行(锁 %s 存在且新鲜);并发生成会交错覆盖 Go/dist/位序状态,等它结束或确认崩溃后删锁重试", lockPath)
		}
		// 陈旧锁:改名夺取(原子,并发夺取只有一个成功),成功者清走残留后重试创建。
		stale := fmt.Sprintf("%s.stale.%d.%d", lockPath, os.Getpid(), time.Now().UnixNano())
		if rerr := os.Rename(lockPath, stale); rerr != nil {
			continue // 输给并发夺取者或持有者恰好释放:回到循环重新评估
		}
		_ = os.Remove(stale)
	}
	return nil, fmt.Errorf("生成锁 %s 竞争失败", lockPath)
}

// exitCleanup 在 fatalf 退出前执行(os.Exit 不跑 defer;当前唯一用途 = 释放生成锁,
// 否则任意校验失败都会把锁留满 10 分钟陈旧期)。
var exitCleanup func()

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	if exitCleanup != nil {
		exitCleanup()
	}
	os.Exit(1)
}
