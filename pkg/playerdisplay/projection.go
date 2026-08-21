// Package playerdisplay 负责组装跨服务的公开玩家展示信息。
// 本包刻意不回退账号登录名：两个权威展示字段均不可用时，最终仅展示 player_id。
package playerdisplay

import (
	"context"
	"sync"

	"github.com/luyuancpp/pandora/pkg/playername"
	"github.com/luyuancpp/pandora/pkg/playerno"
)

// MaxConcurrency 限制一次投影中名字与玩家编号两个权威依赖的总并发数。
const MaxConcurrency = 4

type NameResolver interface {
	ResolvePlayerNames(ctx context.Context, playerIDs []uint64) (map[uint64]string, error)
}

type NoResolver interface {
	ResolvePlayerNos(ctx context.Context, playerIDs []uint64) (map[uint64]uint64, error)
}

type Dependency string

const (
	DependencyPlayerName Dependency = "player_name"
	DependencyPlayerNo   Dependency = "player_no"
)

// Failure 描述一个失败的有界分块。权威依赖失败按弱依赖降级，调用方仅将其用于观测；
// Resolve 只把父 context 取消作为硬错误返回。
type Failure struct {
	Dependency Dependency
	PlayerIDs  []uint64
	Err        error
}

type Projection struct {
	Names    map[uint64]string
	Numbers  map[uint64]uint64
	Failures []Failure
}

type resolveJob struct {
	index      int
	dependency Dependency
	playerIDs  []uint64
}

type resolveResult struct {
	names   map[uint64]string
	numbers map[uint64]uint64
	err     error
}

// Resolve 对非零 ID 稳定去重，按各依赖的协议上限分块，并在同一个有界任务池中
// 并发执行名字与玩家编号两个独立弱依赖。
func Resolve(
	ctx context.Context,
	rawPlayerIDs []uint64,
	nameResolver NameResolver,
	noResolver NoResolver,
) (Projection, error) {
	if err := ctx.Err(); err != nil {
		return Projection{}, err
	}
	playerIDs := stableUniqueNonZero(rawPlayerIDs)
	projection := Projection{
		Names:   make(map[uint64]string, len(playerIDs)),
		Numbers: make(map[uint64]uint64, len(playerIDs)),
	}
	if len(playerIDs) == 0 || (nameResolver == nil && noResolver == nil) {
		return projection, nil
	}

	nameChunks := chunk(playerIDs, playername.ResolveBatchLimit)
	noChunks := chunk(playerIDs, playerno.ResolveBatchLimit)
	jobs := make([]resolveJob, 0, len(nameChunks)+len(noChunks))
	chunkCount := len(nameChunks)
	if len(noChunks) > chunkCount {
		chunkCount = len(noChunks)
	}
	for i := 0; i < chunkCount; i++ {
		if nameResolver != nil && i < len(nameChunks) {
			jobs = append(jobs, resolveJob{dependency: DependencyPlayerName, playerIDs: nameChunks[i]})
		}
		if noResolver != nil && i < len(noChunks) {
			jobs = append(jobs, resolveJob{dependency: DependencyPlayerNo, playerIDs: noChunks[i]})
		}
	}
	for i := range jobs {
		jobs[i].index = i
	}

	results := make([]resolveResult, len(jobs))
	jobCh := make(chan resolveJob, len(jobs))
	for _, job := range jobs {
		jobCh <- job
	}
	close(jobCh)

	workerCount := MaxConcurrency
	if len(jobs) < workerCount {
		workerCount = len(jobs)
	}
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobCh:
					if !ok {
						return
					}
					switch job.dependency {
					case DependencyPlayerName:
						results[job.index].names, results[job.index].err =
							nameResolver.ResolvePlayerNames(ctx, job.playerIDs)
					case DependencyPlayerNo:
						results[job.index].numbers, results[job.index].err =
							noResolver.ResolvePlayerNos(ctx, job.playerIDs)
					}
				}
			}
		}()
	}
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return Projection{}, err
	}

	for index, result := range results {
		job := jobs[index]
		if result.err != nil {
			projection.Failures = append(projection.Failures, Failure{
				Dependency: job.dependency,
				PlayerIDs:  append([]uint64(nil), job.playerIDs...),
				Err:        result.err,
			})
			continue
		}
		requested := make(map[uint64]struct{}, len(job.playerIDs))
		for _, playerID := range job.playerIDs {
			requested[playerID] = struct{}{}
		}
		for playerID, nickname := range result.names {
			if _, ok := requested[playerID]; ok {
				projection.Names[playerID] = nickname
			}
		}
		for playerID, playerNo := range result.numbers {
			if _, ok := requested[playerID]; ok {
				projection.Numbers[playerID] = playerNo
			}
		}
	}
	return projection, nil
}

func stableUniqueNonZero(raw []uint64) []uint64 {
	seen := make(map[uint64]struct{}, len(raw))
	result := make([]uint64, 0, len(raw))
	for _, playerID := range raw {
		if playerID == 0 {
			continue
		}
		if _, ok := seen[playerID]; ok {
			continue
		}
		seen[playerID] = struct{}{}
		result = append(result, playerID)
	}
	return result
}

func chunk(playerIDs []uint64, limit int) [][]uint64 {
	if len(playerIDs) == 0 || limit <= 0 {
		return nil
	}
	chunks := make([][]uint64, 0, (len(playerIDs)+limit-1)/limit)
	for start := 0; start < len(playerIDs); start += limit {
		end := start + limit
		if end > len(playerIDs) {
			end = len(playerIDs)
		}
		chunks = append(chunks, append([]uint64(nil), playerIDs[start:end]...))
	}
	return chunks
}
