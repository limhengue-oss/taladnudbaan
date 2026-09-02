# config.ps1 - shared paths for all wrapper scripts
# edit here if machine/R/rclone paths change

$RepoDir  = "C:\Users\limhe\OneDrive\Documents\GitHub\taladnudbaan"
$Rscript  = "C:\Program Files\R\R-4.6.1\bin\x64\Rscript.exe"
$Rclone   = (Get-ChildItem "C:\Users\limhe\AppData\Local\Microsoft\WinGet\Packages" -Filter "rclone.exe" -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1).FullName
$LogDir   = Join-Path $RepoDir "local\logs"
$RemoteDir = "gdrive:taladnudbaan"

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
