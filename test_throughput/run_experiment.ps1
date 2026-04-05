Set-Location $PSScriptRoot
Set-Location ..

$projectRoot = (Get-Location).Path

$servers = @(
    @{id="1"; port="5001"; dir="data/s1"},
    @{id="2"; port="5002"; dir="data/s2"},
    @{id="3"; port="5003"; dir="data/s3"}
)

$workersList = @(1, 2, 4, 8)

# -------- SETUP --------
New-Item -ItemType Directory -Force -Path "data/s1","data/s2","data/s3","output" | Out-Null

# Track processes
$serverProcesses = @()

function Cleanup {
    Write-Host "Cleaning up processes..."

    foreach ($p in $serverProcesses) {
        if ($p -and !$p.HasExited) {
            Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
        }
    }

    Get-Job | Remove-Job -Force -ErrorAction SilentlyContinue
    $global:serverProcesses = @()

    Start-Sleep -Seconds 1
}

foreach ($w in $workersList) {

    Write-Host "Running experiment with $w worker(s)"

    Cleanup

    Write-Host "Starting servers..."

    foreach ($s in $servers) {
        $cmd = "cd `"$projectRoot`"; go run ./test_throughput/server --id=$($s.id) --port=$($s.port) --dir=$($s.dir)"

        $proc = Start-Process powershell `
            -ArgumentList "-NoExit -Command $cmd" `
            -PassThru

        $serverProcesses += $proc
    }

    Start-Sleep -Seconds 4

    Write-Host "Starting $w workers..."

    $start = Get-Date
    $jobs = @()

    for ($i=1; $i -le $w; $i++) {
        $jobs += Start-Job -ScriptBlock {
            param($i, $root)

            Set-Location $root
            go run ./test_throughput/client --worker=$i

        } -ArgumentList $i, $projectRoot
    }

    # Wait for workers
    $jobs | Wait-Job | Out-Null

    # Print outputs
    foreach ($job in $jobs) {
        Receive-Job $job
    }

    $end = Get-Date
    $elapsed = ($end - $start).TotalSeconds

    Write-Host "Workers: $w | Total Time: $elapsed seconds"

    Cleanup
    Start-Sleep -Seconds 2
}

Write-Host "All experiments completed."