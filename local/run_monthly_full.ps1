# run_monthly_full.ps1 - manual full re-scrape (audit / cross-check vs incremental pipeline)
# Run this again and again until it says "SCRAPE ครบทุกรายการแล้ว" - it resumes automatically.
# Output stays local only (no Drive upload) - see $RepoDir for
#   taladnudbaan_properties_full_<timestamp>.csv  (finished result)
#   taladnudbaan_full_pending.RData                (resume checkpoint, deleted when done)
. "$PSScriptRoot\config.ps1"

$stamp   = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = Join-Path $LogDir "monthly_full_$stamp.log"

Start-Transcript -Path $logFile -Append | Out-Null
try {
    Set-Location $RepoDir

    Write-Output "=== run scrape_monthly_full.R ==="
    & $Rscript "scrape_monthly_full.R"
    $exitCode = $LASTEXITCODE

    if ($exitCode -eq 0) {
        if (Test-Path "taladnudbaan_full_pending.RData") {
            Write-Output "=== not finished yet - run this script again to continue ==="
        } else {
            Write-Output "=== finished - see taladnudbaan_properties_full_*.csv in $RepoDir ==="
        }
        Write-Output "=== DONE (exit $exitCode) ==="
    } else {
        Write-Output "=== scrape_monthly_full.R FAILED (exit $exitCode) ==="
    }
}
finally {
    Stop-Transcript | Out-Null
}
