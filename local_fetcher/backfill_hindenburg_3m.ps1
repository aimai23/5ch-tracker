param(
  [int]$Days = 95
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

python .\backfill_hindenburg_3m.py --days $Days
exit $LASTEXITCODE
