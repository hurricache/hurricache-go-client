param([string]$Protoc = "protoc")
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
if ((& $Protoc --version) -ne "libprotoc 34.1") { throw "protoc 34.1 required" }
$env:GOBIN = Join-Path (Get-Location) ".tools"
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
if ($LASTEXITCODE) { throw "protobuf generator installation failed" }
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
if ($LASTEXITCODE) { throw "gRPC generator installation failed" }
$env:PATH = "$env:GOBIN;$env:PATH"
& $Protoc -I proto --go_out=. --go_opt=module=github.com/hurricache/hurricache-go-client --go-grpc_out=. --go-grpc_opt=module=github.com/hurricache/hurricache-go-client proto/cache.proto proto/coordinator.proto
if ($LASTEXITCODE) { throw "generation failed" }
