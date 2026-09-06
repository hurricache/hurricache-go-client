param(
 [Parameter(Mandatory=$true)][string]$JavaCheckout,
 [Parameter(Mandatory=$true)][string]$JavaHome,
 [Parameter(Mandatory=$true)][string]$Target
)
$ErrorActionPreference="Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$env:JAVA_HOME=$JavaHome
$env:PATH=(Join-Path $JavaHome "bin")+";"+$env:PATH
$work=Join-Path (Get-Location) ".work/java-interop"
New-Item -ItemType Directory -Force $work | Out-Null
if ((git -C $JavaCheckout rev-parse HEAD) -ne "fd2f89d56751b000a62565ef3ff0712040d5bbfd"){throw "Java baseline mismatch"}
[xml]$pom=Get-Content -Raw (Join-Path $JavaCheckout "pom.xml")
$build=$pom.project.build
$pom.project.RemoveChild($build) | Out-Null
$pom.Save((Join-Path $work "pom.xml"))
mvn -q -f (Join-Path $work "pom.xml") dependency:build-classpath "-Dmdep.outputFile=classpath.txt"
if($LASTEXITCODE){throw "dependency resolution failed"}
$cp=(Get-Content -Raw (Join-Path $work "classpath.txt")).Trim()
$sources=@()
$sources+=Get-ChildItem (Join-Path $JavaCheckout "src/main/java/com/hurricache/client/intf") -Filter *.java | Select-Object -ExpandProperty FullName
$sources+=Get-ChildItem (Join-Path $JavaCheckout "src/main/java/com/hurricache/utils") -Filter *.java | Select-Object -ExpandProperty FullName
$sources+=Join-Path $JavaCheckout "src/main/java/com/hurricache/client/FastCacheAsyncSimpleClient.java"
$sources+=Join-Path $JavaCheckout "src/main/java/com/hurricache/client/KeyValueUtils.java"
$generated=Join-Path $work "generated"
New-Item -ItemType Directory -Force $generated | Out-Null
$protoc=Join-Path $JavaCheckout "target/protoc-plugins/protoc-4.34.1-windows-x86_64.exe"
$grpcPlugin=Join-Path $JavaCheckout "target/protoc-plugins/protoc-gen-grpc-java-1.80.0-windows-x86_64.exe"
$protoDir=Join-Path $JavaCheckout "src/main/proto"
& $protoc "-I$protoDir" "--java_out=$generated" "--grpc-java_out=$generated" "--plugin=protoc-gen-grpc-java=$grpcPlugin" (Join-Path $protoDir "cache.proto") (Join-Path $protoDir "coordinator.proto")
if($LASTEXITCODE){throw "Java binding generation failed"}
$sources+=Get-ChildItem $generated -Recurse -Filter *.java | Select-Object -ExpandProperty FullName
$sources+=Join-Path $PSScriptRoot "interop/Interop.java"
$quotedSources=$sources | ForEach-Object { '"'+$_.Replace('\','/')+'"' }
Set-Content -Path (Join-Path $work "sources.txt") -Value $quotedSources
javac -encoding UTF-8 -cp $cp -d $work ("@"+(Join-Path $work "sources.txt"))
if($LASTEXITCODE){throw "Java compile failed"}
$prefix="go-java-parity/"+[guid]::NewGuid().ToString("N")
$hints=Join-Path $work "hints.txt"
go run ./tools/interop write $Target "$prefix/go" $hints
if($LASTEXITCODE){throw "Go fixture write failed"}
java -cp "$work;$cp" Interop read $Target "$prefix/go" $hints
if($LASTEXITCODE){throw "Java fixture read failed"}
go run ./tools/interop cleanup $Target "$prefix/go" $hints
if($LASTEXITCODE){throw "fixture cleanup failed"}
java -cp "$work;$cp" Interop write $Target "$prefix/java" $hints
if($LASTEXITCODE){throw "Java fixture write failed"}
go run ./tools/interop read $Target "$prefix/java" $hints
if($LASTEXITCODE){throw "Go fixture read failed"}
go run ./tools/interop cleanup $Target "$prefix/java" $hints
if($LASTEXITCODE){throw "fixture cleanup failed"}
