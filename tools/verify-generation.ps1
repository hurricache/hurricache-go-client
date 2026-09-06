param([string]$Protoc="protoc")
$ErrorActionPreference="Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$files=Get-ChildItem proto -Recurse -Filter *.pb.go
function HashFile([string]$path) {
 $sha=[System.Security.Cryptography.SHA256]::Create()
 try { return [System.BitConverter]::ToString($sha.ComputeHash([System.IO.File]::ReadAllBytes($path))) }
 finally { $sha.Dispose() }
}
$before=@{}
foreach($f in $files){$before[$f.FullName]=HashFile $f.FullName}
& "$PSScriptRoot/generate.ps1" -Protoc $Protoc
foreach($f in $files){if($before[$f.FullName] -ne (HashFile $f.FullName)){throw "Generated output differs: $($f.Name)"}}
Write-Output "Generated bindings are byte-for-byte reproducible."
