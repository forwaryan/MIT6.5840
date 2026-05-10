param(
    [int]$Runs = 20,
    [string]$Timeout = "90s",
    [switch]$KeepTemp
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$srcDir = Join-Path $repoRoot "src"
if (!(Test-Path $srcDir)) {
    throw "Cannot find src directory at $srcDir"
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$resultDir = Join-Path $repoRoot "test-results/lab5a-$timestamp"
New-Item -ItemType Directory -Force -Path $resultDir | Out-Null

$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("mit65840-lab5a-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $tempRoot | Out-Null

$failures = @()

try {
    Copy-Item -Path $srcDir -Destination $tempRoot -Recurse
    $testDir = Join-Path $tempRoot "src/shardkv"

    for ($i = 1; $i -le $Runs; $i++) {
        $logPath = Join-Path $resultDir ("run-{0:D2}.log" -f $i)
        Write-Host ("[{0}/{1}] Running Lab5A tests..." -f $i, $Runs)

        Push-Location $testDir
        try {
            & go test -run "5A" -count=1 -v -timeout $Timeout *>&1 | Tee-Object -FilePath $logPath
            $exitCode = $LASTEXITCODE
        } finally {
            Pop-Location
        }

        if ($exitCode -eq 0) {
            Write-Host ("[{0}/{1}] PASS" -f $i, $Runs) -ForegroundColor Green
        } else {
            Write-Host ("[{0}/{1}] FAIL, log: {2}" -f $i, $Runs, $logPath) -ForegroundColor Red
            $failures += $i
        }
    }

    Write-Host ""
    Write-Host "Logs: $resultDir"
    if ($failures.Count -eq 0) {
        Write-Host ("All {0} Lab5A runs passed." -f $Runs) -ForegroundColor Green
        exit 0
    }

    Write-Host ("Failed runs: {0}" -f ($failures -join ", ")) -ForegroundColor Red
    exit 1
} finally {
    if (!$KeepTemp -and (Test-Path $tempRoot)) {
        Remove-Item -LiteralPath $tempRoot -Recurse -Force
    } elseif ($KeepTemp) {
        Write-Host "Temp copy kept at: $tempRoot"
    }
}
