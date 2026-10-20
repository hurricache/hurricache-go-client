import com.hurricache.client.FastCacheAsyncStandaloneClient;
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
  static byte[] mapEntryKey(int index) {return new byte[]{0,(byte)(index>>>8),(byte)index,(byte)255};}
  public static void main(String[] args) throws Exception {
    String action=args[0],target=args[1],prefix=args[2];Path hintsFile=Path.of(args[3]);
    String[] address=target.split(":");
    FastCacheAsyncStandaloneClient c=new FastCacheAsyncStandaloneClient(address[0],Integer.parseInt(address[1]),77,Duration.ofSeconds(10));
    Map<String,KeyHintData> hints=new LinkedHashMap<>();
    String[] names={"empty","small","at","compressed","random","list","ordered","map","orderedmap","large","largeorderedmap"};
    List<String> failures=new ArrayList<>();
    try {
      if(action.equals("probe-longkey")){
        byte[] k=key(prefix,"longkey");
        var hint=c.createKeyValue(k,null,data(4096,false),Duration.ZERO,77,Duration.ofSeconds(10)).get();
        if(!Arrays.equals(c.getValue(k,hint,77,Duration.ofSeconds(10)).get(),data(4096,false)))throw new AssertionError("long-key value");
        c.remove(k,hint,77,Duration.ofSeconds(10)).get();
        System.out.println("Java compressed long-key round trip passed");
        return;
      }
      if(action.equals("write")){
        for(String name:names) {
          byte[] k=key(prefix,name);KeyHintData h;
          if(name.equals("list"))h=c.createList(k,null,List.of(Payload.of(data(20,false)),Payload.of(data(4096,false))),Duration.ZERO,77,Duration.ofSeconds(10)).get();
          else if(name.equals("largeorderedmap")){
            Map<OrderedPayload,Payload> entries=new LinkedHashMap<>();
            for(int i=0;i<1024;i++)entries.put(OrderedPayload.of((long)i+1,mapEntryKey(i)),Payload.of(data(4096,true)));
            h=c.createOrderedMap(k,null,entries,Duration.ZERO,77,Duration.ofSeconds(30)).get();
          }
          else if(name.equals("ordered"))h=c.createOrderedSet(k,null,List.of(OrderedPayload.of(1L,data(4096,false)),OrderedPayload.of(-1L,data(4097,false))),Duration.ZERO,77,Duration.ofSeconds(10)).get();
          else if(name.equals("large"))h=c.createList(k,null,Collections.nCopies(1024,Payload.of(data(4096,true))),Duration.ZERO,77,Duration.ofSeconds(30)).get();
          else if(name.equals("orderedmap"))h=c.createOrderedMap(k,null,Map.of(OrderedPayload.of(1L,data(4096,false)),Payload.of(data(4096,true)),OrderedPayload.of(2L,data(4097,false)),Payload.of(data(4097,true))),Duration.ZERO,77,Duration.ofSeconds(10)).get();
          else if(name.equals("map"))h=c.createMap(k,null,Map.of(Payload.of(bytes("entry")),Payload.of(data(4096,false))),Duration.ZERO,77,Duration.ofSeconds(10)).get();
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
            if(vals.size()!=2 || vals.get(1).getOrder()!=-1L || !Arrays.equals(vals.get(0).getValue(),data(4096,false)) || !Arrays.equals(vals.get(1).getValue(),data(4097,false)))throw new AssertionError("unsigned ordered");
          } else if(name.equals("large")){
            var vals=c.streamList(k,h,77,Duration.ofSeconds(30)).get();
            if(vals.size()!=1024 || vals.stream().anyMatch(v -> !Arrays.equals(v.getValue(),data(4096,true))))throw new AssertionError("large continuation");
          } else if(name.equals("orderedmap")){
            for(boolean reverse:new boolean[]{false,true}) {
              var vals=c.streamElementInRangeOrderedMap(k,h,1L,2L,reverse,77,Duration.ofSeconds(10)).get();
              if(vals.size()!=2)throw new AssertionError("ordered map range");
              long expectedOrder=reverse?2L:1L;
              for(var entry:vals.entrySet()) {
                if(entry.getKey().getOrder()!=expectedOrder)throw new AssertionError("ordered map iteration order");
                expectedOrder+=reverse?-1L:1L;
                int size=entry.getKey().getOrder()==1L?4096:4097;
                if(!Arrays.equals(entry.getKey().getValue(),data(size,false)) || !Arrays.equals(entry.getValue().getValue(),data(size,true))) {
                  String failure="ordered map "+System.getProperty("hurricache.compression")+": reverse="+reverse+" weight="+entry.getKey().getOrder()+" keyLength="+entry.getKey().getValue().length+" valueLength="+entry.getValue().getValue().length+" expectedLength="+size+" keyPrefix="+HexFormat.of().formatHex(Arrays.copyOf(entry.getKey().getValue(),Math.min(12,entry.getKey().getValue().length)));
                  System.err.println(failure); failures.add(failure);
                }
              }
            }
          } else if(name.equals("largeorderedmap")){
            for(boolean reverse:new boolean[]{false,true}){
              var entries=c.streamElementInRangeOrderedMap(k,h,1L,1024L,reverse,77,Duration.ofSeconds(30)).get();
              if(entries.size()!=1024)throw new AssertionError("large ordered map count");
              long expected=reverse?1024L:1L;
              for(var entry:entries.entrySet()){
                if(entry.getKey().getOrder()!=expected || !Arrays.equals(entry.getKey().getValue(),mapEntryKey((int)expected-1)) || !Arrays.equals(entry.getValue().getValue(),data(4096,true)))throw new AssertionError("large ordered map bytes/order");
                expected+=reverse?-1L:1L;
              }
            }
          } else if(name.equals("map")){
            byte[] v=c.getContainerValue(k,h,bytes("entry"),77,Duration.ofSeconds(10)).get();
            if(!Arrays.equals(v,data(4096,false)))throw new AssertionError("map");
          } else {
            int n=name.equals("empty")?0:name.equals("small")?1023:name.equals("at")?1024:4096;
            if(!Arrays.equals(c.getValue(k,h,77,Duration.ofSeconds(10)).get(),data(n,name.equals("random"))))throw new AssertionError(name);
          }
        }
      }
      if(!failures.isEmpty())throw new AssertionError(String.join("; ",failures));
      System.out.println("Java "+action+" interoperability passed");
    } finally {c.shutdown();}
  }
}
