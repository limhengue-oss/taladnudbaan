# run_resume.ps1 - equivalent to scrape-resume.yml (resume from pending queue, no new list scrape)
. "$PSScriptRoot\config.ps1"

$stamp   = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = Join-Path $LogDir "resume_$stamp.log"

Start-Transcript -Path $logFile -Append | Out-Null
try {
    Set-Location $RepoDir

    Write-Output "=== download baseline + pending queue ==="
    & $Rclone copy "$RemoteDir/taladnudbaan_urls.RData" $RepoDir
    & $Rclone copy "$RemoteDir/taladnudbaan_pending.RData" $RepoDir

    Write-Output "=== resume scrape_daily.R (RESUME_ONLY=true) ==="
    $env:WORKFLOW_START_EPOCH = [int][double]::Parse((Get-Date -UFormat %s))
    $env:RESUME_ONLY = "true"
    & $Rscript "scrape_daily.R"
    $exitCode = $LASTEXITCODE

    if ($exitCode -eq 0) {
        Write-Output "=== upload output files ==="
        if (Test-Path "output_files.txt") {
            Get-Content "output_files.txt" | Where-Object { $_.Trim() -ne "" } | ForEach-Object {
                & $Rclone copy $_ "$RemoteDir/"
            }
        }
        & $Rclone copy "taladnudbaan_urls.RData" "$RemoteDir/"
        & $Rclone copy "update_status.txt" "$RemoteDir/"
        if (Test-Path "taladnudbaan_pending.RData") {
            & $Rclone copy "taladnudbaan_pending.RData" "$RemoteDir/"
        } else {
            & $Rclone deletefile "$RemoteDir/taladnudbaan_pending.RData"
        }
        Write-Output "=== DONE (exit $exitCode) ==="
    } else {
        Write-Output "=== scrape_daily.R FAILED (exit $exitCode) - skip upload ==="
    }
}
finally {
    Stop-Transcript | Out-Null
}
