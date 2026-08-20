# Pandora Proto 代码生成
#
# 用法:
#   pwsh tools/scripts/proto_gen.ps1            # buf lint + 生成 go pb
#   pwsh tools/scripts/proto_gen.ps1 -Lint      # 只 lint
#   pwsh tools/scripts/proto_gen.ps1 -Cpp       # 同时生成 cpp pb(给 UE 仓库用)
#   pwsh tools/scripts/proto_gen.ps1 -Breaking  # 检测 breaking change(对比 main 分支)
#
# 前置:必须装 buf。安装方式:
#   winget install bufbuild.buf
#   或 scoop install buf
#   或下载 https://github.com/bufbuild/buf/releases
#
# 第一次 buf generate 会从 buf.build 拉远程插件(protoc-gen-go / protoc-gen-go-grpc 等),
# 需要外网。
#
# Kratos HTTP 代码生成使用本地 protoc-gen-go-http(不是 BSR 远程插件),
# 需确保 PATH 可找到该命令。安装方式:
#   go install github.com/go-kratos/kratos/cmd/protoc-gen-go-http/v2@latest

param(
    [switch]$Lint,
    [switch]$Cpp,
    [switch]$Breaking
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot/../.."
$ProtoDir    = "$ProjectRoot/proto"

# 检查 buf
$bufCmd = Get-Command buf -ErrorAction SilentlyContinue
if ($null -eq $bufCmd) {
    Write-Host "[ERR] buf 未安装" -ForegroundColor Red
    Write-Host "  请运行下面任一命令安装:"
    Write-Host "    winget install bufbuild.buf"
    Write-Host "    scoop install buf"
    Write-Host "  或访问 https://github.com/bufbuild/buf/releases"
    exit 1
}

# 检查 Kratos HTTP plugin(仅 generate go 时需要)
if (-not $Lint) {
    $kratosHttpCmd = Get-Command protoc-gen-go-http -ErrorAction SilentlyContinue
    if ($null -eq $kratosHttpCmd) {
        Write-Host "[ERR] protoc-gen-go-http 未安装或不在 PATH" -ForegroundColor Red
        Write-Host "  请先安装 Go,然后运行:"
        Write-Host "    go install github.com/go-kratos/kratos/cmd/protoc-gen-go-http/v2@latest"
        exit 1
    }
}

Write-Host "===== Pandora proto gen =====" -ForegroundColor Cyan
Write-Host "buf:   $($bufCmd.Source)"
Write-Host "proto: $ProtoDir"
if (-not $Lint) {
    Write-Host "http:  $($kratosHttpCmd.Source)"
}

Push-Location $ProtoDir
try {
    # 1. lint
    Write-Host ""
    Write-Host "[1] buf lint" -ForegroundColor Yellow
    & buf lint
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERR] buf lint failed" -ForegroundColor Red
        exit 1
    }
    Write-Host "  OK" -ForegroundColor Green

    if ($Lint) {
        Write-Host ""
        Write-Host "(仅 lint 模式,跳过 generate)" -ForegroundColor DarkGray
        return
    }

    # 2. breaking(可选)
    if ($Breaking) {
        Write-Host ""
        Write-Host "[2] buf breaking against main" -ForegroundColor Yellow
        # buf 从 $ProtoDir(proto/)运行,但 --against 指向仓库根 .git;必须带 subdir=proto,
        # 否则 buf 会拿仓库根(无 buf.yaml 模块)当基线,对不上当前模块 → breaking 检测形同虚设。
        #
        # GIT_LFS_SKIP_SMUDGE=1(审核 P1 #9):buf 读取 .git 基线时 git 会对整棵树跑 LFS smudge,
        # 本仓库含 171MB 离线镜像等 LFS 大对象,LFS remote 不可用时 smudge 报错会连累 breaking 检查失败。
        # proto 文件本身不是 LFS 对象,跳过 smudge 不影响基线 proto 内容,只避免拉取无关 LFS blob。
        $prevSkipSmudge = $env:GIT_LFS_SKIP_SMUDGE
        $env:GIT_LFS_SKIP_SMUDGE = '1'
        try {
            & buf breaking --against "$ProjectRoot/.git#branch=main,subdir=proto"
        }
        finally {
            $env:GIT_LFS_SKIP_SMUDGE = $prevSkipSmudge
        }
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERR] breaking change detected!" -ForegroundColor Red
            exit 1
        }
        Write-Host "  OK" -ForegroundColor Green
    }

    # 3. generate go
    Write-Host ""
    Write-Host "[3] buf generate go" -ForegroundColor Yellow
    & buf generate --template buf.gen.go.yaml
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERR] go generate failed" -ForegroundColor Red
        exit 1
    }
    Write-Host "  OK → $ProtoDir/gen/go/" -ForegroundColor Green

    # 3.5 generate python
    #
    # ★ 必须和 go 一起生成,不能"要用的时候再手动跑一次"。
    # python/gen/ 与 proto/gen/go 一样是**入库的生成物**,而这里是全仓唯一的生成入口。
    # 漏掉它的后果是静默的:改完 proto 只重生成了 Go,Python 侧还在用旧 stub ——
    # 加字段时表现为"Python 读不到新字段"(不报错,值是默认值),改字段号时表现为
    # **串字段**。而 CI 的 Python 门跑的是旧 stub,照样全绿。
    Write-Host ""
    Write-Host "[3.5] buf generate python" -ForegroundColor Yellow
    & buf generate --template buf.gen.python.yaml
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERR] python generate failed" -ForegroundColor Red
        exit 1
    }
    Write-Host "  OK → $ProjectRoot/python/gen/" -ForegroundColor Green

    # 4. generate cpp(可选)
    if ($Cpp) {
        Write-Host ""
        Write-Host "[4] buf generate cpp" -ForegroundColor Yellow
        & buf generate --template buf.gen.cpp.yaml
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERR] cpp generate failed" -ForegroundColor Red
            exit 1
        }
        # 部分 protoc C++ 版本会在换行前输出单个空格，导致 git diff --check 失败。
        # 统一生成入口内做确定性规范化，避免手改生成文件后下次生成又漂移。
        Get-ChildItem -Path "$ProtoDir/gen/cpp" -File -Recurse -Include *.cc, *.h | ForEach-Object {
            $content = [System.IO.File]::ReadAllText($_.FullName)
            $normalized = [System.Text.RegularExpressions.Regex]::Replace(
                $content,
                '[ \t]+(?=\r?$)',
                '',
                [System.Text.RegularExpressions.RegexOptions]::Multiline
            )
            if ($normalized -cne $content) {
                [System.IO.File]::WriteAllText(
                    $_.FullName,
                    $normalized,
                    [System.Text.UTF8Encoding]::new($false)
                )
            }
        }
        Write-Host "  OK → $ProtoDir/gen/cpp/" -ForegroundColor Green
    }

    # 5. 统计
    Write-Host ""
    Write-Host "===== 产物 =====" -ForegroundColor Green
    $goFiles = Get-ChildItem -Path "$ProtoDir/gen/go" -Filter *.go -Recurse -ErrorAction SilentlyContinue
    Write-Host "go pb:  $($goFiles.Count) files"
    if ($Cpp) {
        $cppFiles = Get-ChildItem -Path "$ProtoDir/gen/cpp" -Filter *.cc -Recurse -ErrorAction SilentlyContinue
        Write-Host "cpp pb: $($cppFiles.Count) files"
    }
} finally {
    Pop-Location
}
