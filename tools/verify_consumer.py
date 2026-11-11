"""Build a source module archive and test an isolated extracted Go consumer."""

import os
import subprocess
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / ".work" / "release-0.2.0"
MODULE = "github.com/hurricache/hurricache-go-client"

TEST = r"""package consumer_test

import (
 "context"
 "errors"
 "math"
 "testing"
 hc "github.com/hurricache/hurricache-go-client"
 pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
 "google.golang.org/grpc"
 "google.golang.org/protobuf/proto"
)

type wire struct{value *pb.Value}
func(w *wire)Invoke(_ context.Context,method string,req,reply any,_ ...grpc.CallOption)error{
 switch r:=req.(type){
 case *pb.CreateRequest:w.value=proto.Clone(r.Value).(*pb.Value);proto.Merge(reply.(proto.Message),&pb.KeyHintResponse{Relayed:hc.Ptr(true)})
 case *pb.GetRequest:proto.Merge(reply.(proto.Message),&pb.ValueResponse{ValueUnordered:w.value})
 case *pb.AtomicCas:
  if r.Keys==nil||r.Expected.Ttl!=nil||r.ToSet.Ttl!=nil{return errors.New("container CAS selector/TTL")}
  proto.Merge(reply.(proto.Message),&pb.AtomicCasRes{Result:true})
 default:return errors.New(method)
 };return nil
}
func(w *wire)NewStream(context.Context,*grpc.StreamDesc,string,...grpc.CallOption)(grpc.ClientStream,error){return nil,errors.New("no stream needed")}
func TestInstalledModule(t *testing.T){
 if hc.Version!="0.2.0"{t.Fatal(hc.Version)}
 for _,codec:=range []hc.CompressionCodec{hc.CompressionGZIP,hc.CompressionLZ4}{
  c,e:=hc.NewClientWithConnection(&wire{},hc.Config{CompressionCodec:codec,CompressionThreshold:hc.Ptr(0)});if e!=nil{t.Fatal(e)}
  var _ hc.Operations=c
  for _,selector:=range []any{[]byte("field"),hc.OrderedPayload{Payload:hc.Payload{Data:[]byte("field")},Order:hc.Ptr(uint64(0))}}{r,e:=c.CompareAndSetInContainer(context.Background(),[]byte("map"),selector,int64(1),int64(2),hc.Options{});if e!=nil||!r.Swapped||r.Expected!=nil{t.Fatal(r,e)}}
  values:=[]hc.Variant{hc.None(),hc.Bytes(nil),hc.Int(math.MinInt64),hc.Double(math.Copysign(0,-1)),hc.String("compressed bytes")}
  for _,v:=range values{hint,e:=c.Create(context.Background(),[]byte("k"),v,hc.Options{ClientID:hc.Ptr(uint32(0))});if e!=nil||!hint.WasRelayed{t.Fatal(e)};got,e:=c.Get(context.Background(),[]byte("k"),hc.Options{});if e!=nil||got!=v{t.Fatal(got,e)}}
  ov:=hc.Ordered(hc.Int(1),nil);if !ov.IsInt()||ov.Order()!=nil||ov.Value()!=hc.Int(1){t.Fatal("ordered public accessors")};if n,e:=ov.Int64();e!=nil||n!=1{t.Fatal(n,e)};m:=map[hc.OrderedVariant]bool{ov:true};if !m[hc.Ordered(hc.Int(1),nil)]{t.Fatal("comparable ordered model")}
  pool,e:=hc.NewJedisPool(c);if e!=nil{t.Fatal(e)};resource,e:=pool.GetResource();if e!=nil||resource.Ping()!="PONG"{t.Fatal(e)};if e=resource.Close();e!=nil{t.Fatal(e)};if e=pool.Close();e!=nil{t.Fatal(e)}
 }
 var _ hc.Operations=(*hc.SmartClient)(nil)
}
"""


def main():
    WORK.mkdir(parents=True, exist_ok=True)
    archive = WORK / "hurricache-go-client-v0.2.0.zip"
    files = [
        ROOT / name
        for name in (
            "go.mod",
            "go.sum",
            "README.md",
            "API_REFERENCE.md",
            "API_PARITY.md",
            "FINDINGS.md",
        )
    ]
    files += [p for p in ROOT.glob("*.go") if not p.name.endswith("_test.go")]
    files += list((ROOT / "proto").rglob("*.go"))
    files += list((ROOT / "proto").glob("*.proto"))
    prefix = MODULE + "@v0.2.0/"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for path in files:
            bundle.write(path, prefix + path.relative_to(ROOT).as_posix())
    extracted = WORK / "extracted"
    with zipfile.ZipFile(archive) as bundle:
        bundle.extractall(extracted)
    module = extracted / (MODULE + "@v0.2.0")
    consumer = WORK / "consumer"
    consumer.mkdir(exist_ok=True)
    (consumer / "go.mod").write_text(
        f"module example.com/isolated-hurricache-consumer\n\ngo 1.26.0\n\nrequire {MODULE} v0.2.0\n\nreplace {MODULE} => {module.as_posix()}\n",
        encoding="utf-8",
    )
    (consumer / "installed_test.go").write_text(TEST, encoding="utf-8")
    env = dict(os.environ, GOWORK="off", GOPROXY="off")
    subprocess.run(
        ["go", "test", "-mod=mod", "-count=1", "."], cwd=consumer, env=env, check=True
    )
    print("Isolated archived-module consumer passed with GZIP and raw LZ4.")
    print(archive.relative_to(ROOT))


if __name__ == "__main__":
    main()
