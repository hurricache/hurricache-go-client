import com.hurricache.client.FastCacheAsyncSimpleClient;
import com.hurricache.client.intf.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

// Compiled outside the authoritative Java checkout by tools/interop.ps1.
public class Interop {
  static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
  static byte[] data(int size, boolean random) {
    byte[] b=new byte[size];int x=1234567;
    for(int i=0;i<size;i++){x^=x<<13;x^=x>>>17;x^=x<<5;b[i]=random?(byte)x:(byte)42;}return b;
  }
  static byte[] key(String prefix,String name) {return bytes(prefix+"/"+name+(name.equals("longkey")?"/"+"x".repeat(2048):""));}
  public static void main(String[] args) throws Exception {
    String action=args[0],target=args[1],prefix=args[2];Path hintsFile=Path.of(args[3]);
    String[] address=target.split(":");
    FastCacheAsyncSimpleClient c=new FastCacheAsyncSimpleClient(address[0],Integer.parseInt(address[1]),77,Duration.ofSeconds(10));
    Map<String,KeyHintData> hints=new LinkedHashMap<>();
    String[] names={"empty","small","at","compressed","random","list","ordered","map"};
    try {
      if(action.equals("probe-longkey")){
        c.createKeyValue(key(prefix,"longkey"),null,data(4096,false),Duration.ZERO,77,Duration.ofSeconds(10)).get();
        return;
      }
      if(action.equals("write")){
        for(String name:names) {
          byte[] k=key(prefix,name);KeyHintData h;
          if(name.equals("list"))h=c.createList(k,null,List.of(Payload.of(data(20,false)),Payload.of(data(4096,false))),Duration.ZERO,77,Duration.ofSeconds(10)).get();
          else if(name.equals("ordered"))h=c.createOrderedSet(k,List.of(OrderedPayload.of(1L,data(20,false)),OrderedPayload.of(-1L,data(40,false))),Duration.ZERO,77,Duration.ofSeconds(10)).get();
          else if(name.equals("map"))h=c.createMap(k,Map.of(Payload.of(bytes("entry")),Payload.of(data(4096,false))),Duration.ZERO,77,Duration.ofSeconds(10)).get();
          else {
            int n=name.equals("empty")?0:name.equals("small")?1023:name.equals("at")?1024:4096;
            h=c.createKeyValue(k,null,data(n,name.equals("random")),Duration.ZERO,77,Duration.ofSeconds(10)).get();
          }
          hints.put(name,h);
        }
        List<String> lines=new ArrayList<>();hints.forEach((n,h)->lines.add(n+" "+Integer.toUnsignedString(h.getWeek_hash())+" "+Integer.toUnsignedString(h.getStrong_hash())));
        Files.write(hintsFile,lines);
      } else {
        for(String line:Files.readAllLines(hintsFile)){String[] p=line.split(" ");hints.put(p[0],KeyHintData.of(Integer.parseUnsignedInt(p[2]),Integer.parseUnsignedInt(p[1])));}
        for(String name:names){
          byte[] k=key(prefix,name);KeyHintData h=hints.get(name);
          if(name.equals("list")){
            var vals=c.streamList(k,h,77,Duration.ofSeconds(10)).get();
            if(vals.size()!=2 || !Arrays.equals(vals.get(1).getValue(),data(4096,false)))throw new AssertionError("list");
            c.addElementToTail(k,h,List.of(Payload.of(bytes("from-java"))),77,Duration.ofSeconds(10)).get();
          } else if(name.equals("ordered")){
            var vals=c.streamElementInRangeOrderedSet(k,h,0,-1L,false,77,Duration.ofSeconds(10)).get();
            if(vals.size()!=2 || vals.get(1).getOrder()!=-1L)throw new AssertionError("unsigned ordered");
          } else if(name.equals("map")){
            byte[] v=c.getContainerValue(k,h,bytes("entry"),77,Duration.ofSeconds(10)).get();
            if(!Arrays.equals(v,data(4096,false)))throw new AssertionError("map");
          } else {
            int n=name.equals("empty")?0:name.equals("small")?1023:name.equals("at")?1024:4096;
            if(!Arrays.equals(c.getValue(k,h,77,Duration.ofSeconds(10)).get(),data(n,name.equals("random"))))throw new AssertionError(name);
          }
        }
      }
      System.out.println("Java "+action+" interoperability passed");
    } finally {c.shutdown();}
  }
}
