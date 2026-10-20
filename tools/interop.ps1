param(
 [Parameter(Mandatory=$true)][string]$JavaCheckout,
 [Parameter(Mandatory=$true)][string]$JavaHome,
 [string]$Target,
 [ValidateSet("gzip","lz4")][string]$Codec="gzip",
 [switch]$Offline,
 [string]$Protoc,
 [string]$GrpcPlugin
)
$ErrorActionPreference="Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$JavaCheckout=(Resolve-Path $JavaCheckout).Path
$JavaHome=(Resolve-Path $JavaHome).Path
$env:JAVA_HOME=$JavaHome
$env:PATH=(Join-Path $JavaHome "bin")+";"+$env:PATH
$version=& javac -version 2>&1
if($LASTEXITCODE -or "$version" -notmatch 'javac (\d+)' -or [int]$Matches[1] -lt 24){throw "JDK 24 or newer required: $version"}
if(!$Offline -and !$Target){throw "Target is required for live interoperability; use -Offline for codec fixtures"}
$baseline="98225d9365e0ca7cea81e83116ed6f6ddba4294a"
$work=Join-Path (Get-Location) ".work/java-interop/$baseline"
$reference=Join-Path $work "reference"
$classes=Join-Path $work "classes"
New-Item -ItemType Directory -Force $reference,$classes | Out-Null
# Read the exact pin regardless of checkout HEAD; never generate or compile there.
git -C $JavaCheckout archive --format=tar --output="$work/reference.tar" $baseline
if($LASTEXITCODE){throw "Java baseline unavailable"}
tar -xf "$work/reference.tar" -C $reference
if($LASTEXITCODE){throw "Java extraction failed"}
[xml]$pom=Get-Content -Raw (Join-Path $reference "pom.xml")
$pom.project.RemoveChild($pom.project.build) | Out-Null
$pom.Save((Join-Path $work "pom.xml"))
mvn -q -f (Join-Path $work "pom.xml") dependency:build-classpath "-Dmdep.outputFile=classpath.txt"
if($LASTEXITCODE){throw "dependency resolution failed"}
$cp=(Get-Content -Raw (Join-Path $work "classpath.txt")).Trim()
$sources=@()
$sources+=Get-ChildItem (Join-Path $reference "src/main/java/com/hurricache/client/intf") -Filter *.java | Select-Object -ExpandProperty FullName
$sources+=Get-ChildItem (Join-Path $reference "src/main/java/com/hurricache/utils") -Filter *.java | Select-Object -ExpandProperty FullName
$sources+=Join-Path $reference "src/main/java/com/hurricache/client/FastCacheAsyncSimpleClient.java"
$sources+=Join-Path $reference "src/main/java/com/hurricache/client/FastCacheAsyncStandaloneClient.java"
$sources+=Join-Path $reference "src/main/java/com/hurricache/client/KeyValueUtils.java"
$generated=Join-Path $work "generated"
New-Item -ItemType Directory -Force $generated | Out-Null
if(!$Protoc){$Protoc=Join-Path $JavaCheckout "target/protoc-plugins/protoc-4.34.1-windows-x86_64.exe"}
if(!$GrpcPlugin){$GrpcPlugin=Join-Path $JavaCheckout "target/protoc-plugins/protoc-gen-grpc-java-1.82.1-windows-x86_64.exe"}
$protoDir=Join-Path $reference "src/main/proto"
& $Protoc "-I$protoDir" "--java_out=$generated" "--grpc-java_out=$generated" "--plugin=protoc-gen-grpc-java=$GrpcPlugin" (Join-Path $protoDir "cache.proto") (Join-Path $protoDir "coordinator.proto")
if($LASTEXITCODE){throw "Java binding generation failed"}
$sources+=Get-ChildItem $generated -Recurse -Filter *.java | Select-Object -ExpandProperty FullName
$sources+=Join-Path $PSScriptRoot "interop/Interop.java"
$sources+=Join-Path $PSScriptRoot "interop/CodecInterop.java"
$quotedSources=$sources | ForEach-Object { '"'+$_.Replace('\','/')+'"' }
Set-Content -Path (Join-Path $work "sources.txt") -Value $quotedSources
javac -encoding UTF-8 -cp $cp -d $classes ("@"+(Join-Path $work "sources.txt"))
if($LASTEXITCODE){throw "Java compile failed"}
if($Offline){
 $fixtures=Join-Path (Get-Location) "testdata/java-codecs"
 $output=Join-Path $work "go-codecs"
 New-Item -ItemType Directory -Force $fixtures,$output | Out-Null
 foreach($format in @("gzip","lz4")){
  java "-Dhurricache.compression=$format" -cp "$classes;$cp" CodecInterop write (Join-Path $fixtures "$format.pb")
  if($LASTEXITCODE){throw "Java $format fixture generation failed"}
 }
 $previousOutput=$env:HURRICACHE_CODEC_OUTPUT
 try {
  $env:HURRICACHE_CODEC_OUTPUT=$output
  go test -count=1 -run '^TestJavaCodecFixtures$' .
  if($LASTEXITCODE){throw "Go codec interoperability failed"}
 } finally { $env:HURRICACHE_CODEC_OUTPUT=$previousOutput }
 foreach($format in @("gzip","lz4")){
  java "-Dhurricache.compression=$format" -cp "$classes;$cp" CodecInterop read (Join-Path $output "$format.pb")
  if($LASTEXITCODE){throw "Java $format decoding of Go fixtures failed"}
 }
 return
}
$prefix="go-java-parity/"+[guid]::NewGuid().ToString("N")
$hints=Join-Path $work "hints.txt"
$failures=@()
go run ./tools/interop ready $Target $prefix $hints $Codec
if($LASTEXITCODE){throw "Server did not reach gRPC readiness"}
go run ./tools/interop write $Target "$prefix/go" $hints $Codec
if($LASTEXITCODE){throw "Go fixture write failed; discard the disposable server to clean partial fixtures"}
try {
 java "-Dhurricache.compression=$Codec" -cp "$classes;$cp" Interop read $Target "$prefix/go" $hints
 if($LASTEXITCODE){$failures+="Go-to-Java reader failed"}
} finally {
 go run ./tools/interop cleanup $Target "$prefix/go" $hints $Codec
 if($LASTEXITCODE){$failures+="Go fixture cleanup failed"}
}
java "-Dhurricache.compression=$Codec" -cp "$classes;$cp" Interop write $Target "$prefix/java" $hints
if($LASTEXITCODE){throw "Java fixture write failed; discard the disposable server to clean partial fixtures"}
try {
 go run ./tools/interop read $Target "$prefix/java" $hints $Codec
 if($LASTEXITCODE){$failures+="Java-to-Go reader failed"}
} finally {
 go run ./tools/interop cleanup $Target "$prefix/java" $hints $Codec
 if($LASTEXITCODE){$failures+="Java fixture cleanup failed"}
}
if($failures.Count){throw ($failures -join "; ")}
