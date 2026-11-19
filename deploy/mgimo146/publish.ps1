param(
    [string]$SshTarget = 'stp@172.17.0.146',
    [string]$Package = 'tmp/publish-146-20261118/release.tar.gz',
    [switch]$CheckOnly,
    [string]$RollbackStage = ''
)
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
if ($SshTarget -notmatch '^[a-zA-Z0-9_.-]+@172\.17\.0\.146$') { throw 'Expected user@172.17.0.146' }
function Run-Native([string]$exe, [string[]]$arguments) {
    & $exe @arguments
    if ($LASTEXITCODE -ne 0) { throw "$exe failed (exit $LASTEXITCODE)" }
}
$sshArgs = @('-o','BatchMode=yes','-o','ConnectTimeout=15','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=4')
Run-Native 'ssh' ($sshArgs + @($SshTarget, 'test "$(hostname)" = prod-146 && docker info --format "{{.ServerVersion}}" && df -h /'))
if ($RollbackStage) {
    if ($RollbackStage -notmatch '^/home/stp/mgimo-release-[0-9-]+$') { throw 'Invalid rollback stage path' }
    Run-Native 'ssh' ($sshArgs + @($SshTarget, "python3 $RollbackStage/publish_remote.py rollback $RollbackStage"))
    return
}
if (-not [IO.Path]::IsPathRooted($Package)) { $Package = Join-Path $repo $Package }
$Package = (Resolve-Path -LiteralPath $Package).Path
$digest = (Get-FileHash -LiteralPath $Package -Algorithm SHA256).Hash.ToLowerInvariant()
Write-Host "Package: $Package"
Write-Host "SHA256: $digest"
if ($CheckOnly) { Write-Host 'Connectivity and local package checked; server unchanged.'; return }
$stage = '/home/stp/mgimo-release-' + (Get-Date -Format 'yyyyMMdd-HHmmss')
Run-Native 'ssh' ($sshArgs + @($SshTarget, "umask 077; mkdir $stage"))
Run-Native 'scp' ($sshArgs + @($Package, "${SshTarget}:$stage/release.tar.gz"))
foreach ($file in @('publish_remote.py','smoke.py')) {
    Run-Native 'scp' ($sshArgs + @((Join-Path $PSScriptRoot $file), "${SshTarget}:$stage/$file"))
}
Write-Host "Server backup and logs: $stage"
Run-Native 'ssh' ($sshArgs + @($SshTarget, "python3 $stage/publish_remote.py publish $stage $digest"))
Write-Host "Publication complete. Manual rollback: .\deploy\mgimo146\publish.ps1 -RollbackStage '$stage'"
