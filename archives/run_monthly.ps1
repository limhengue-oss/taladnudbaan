# run_monthly.ps1 - equivalent to scrape-monthly.yml (rebuild taladnudbaan_properties.csv from prev month)
. "$PSScriptRoot\config.ps1"

$stamp   = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = Join-Path $LogDir "monthly_$stamp.log"

Start-Transcript -Path $logFile -Append | Out-Null
try {
    Set-Location $RepoDir

    Write-Output "=== download properties + status ==="
    & $Rclone copy "$RemoteDir/taladnudbaan_properties.csv" $RepoDir
    & $Rclone copy "$RemoteDir/update_status.txt" $RepoDir

    $prevMonth = (Get-Date -Day 1).AddDays(-1).ToString("yyyyMM")
    Write-Output "=== download changelog/detail_update for month $prevMonth ==="
    & $Rclone copy $RemoteDir $RepoDir --include "changelog_$prevMonth*.csv"
    & $Rclone copy $RemoteDir $RepoDir --include "detail_update_$prevMonth*.csv"

    Write-Output "=== run scrape_monthly.R ==="
    & $Rscript "scrape_monthly.R"
    $exitCode = $LASTEXITCODE

    if ($exitCode -eq 0) {
        Write-Output "=== upload output files ==="
        & $Rclone copy "taladnudbaan_properties.csv" "$RemoteDir/"
        Get-ChildItem -Filter "taladnudbaan_properties_*.csv" | ForEach-Object {
            & $Rclone copy $_.FullName "$RemoteDir/"
        }
        Write-Output "=== DONE (exit $exitCode) ==="
    } else {
        Write-Output "=== scrape_monthly.R FAILED (exit $exitCode) - skip upload ==="
    }
}
finally {
    Stop-Transcript | Out-Null
}
