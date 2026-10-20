import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.grpc.*;
import com.hurricache.utils.CompressionUtils;
import com.hurricache.utils.StreamBatchOrderedMapObserver;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;

// Offline fixtures use the pinned Java codecs and ordered-map observer directly.
public class CodecInterop {
  static final int[] SIZES = {0, 1, 1023, 1024, 1025, 4096, 16384};
  static byte[] data(int index) { return Interop.data(SIZES[index / 2], index % 2 == 1); }
  static BatchValueResponse produce() {
    var batch = BatchValueResponse.newBuilder();
    for (int i = 0; i < SIZES.length * 2; i++) {
      byte[] raw = data(i);
      batch.addKeyOrdered(CompressionUtils.compressKeyIfNeeded(raw, (long)i + 1, 0, 1024));
      batch.addValueUnordered(CompressionUtils.compressIfNeeded(raw, 1024));
    }
    return batch.build();
  }
  static void verify(BatchValueResponse batch) throws Exception {
    if (batch.getKeyOrderedCount() != SIZES.length * 2) throw new AssertionError("fixture count");
    for (boolean reverse : new boolean[]{false, true}) {
      var future = new CompletableFuture<Map<OrderedPayload, Payload>>();
      var observer = new StreamBatchOrderedMapObserver(future);
      // Three entries per response exercises accumulation and iteration across batches.
      var chunk = BatchValueResponse.newBuilder();
      for (int n = 0; n < batch.getKeyOrderedCount(); n++) {
        int i = reverse ? batch.getKeyOrderedCount() - 1 - n : n;
        var key = batch.getKeyOrdered(i);
        var value = batch.getValueUnordered(i);
        if (key.getClientId() != 0 || !key.hasClientId()) throw new AssertionError("client ID zero");
        boolean compressed = data(i).length > 1024;
        if (key.getCompressionInfo().getEnabled() != compressed || value.getCompressionInfo().getEnabled() != compressed)
          throw new AssertionError("threshold");
        // Also exercise scalar and ordered-value decoding with the same block bytes.
        if (!Arrays.equals(CompressionUtils.decompressIfNeeded(value), data(i))) throw new AssertionError("scalar bytes");
        var ordered = OrderedValue.newBuilder().setValue(value.getValue()).setOrder((long)i + 1);
        if (value.hasCompressionInfo()) ordered.setCompressionInfo(value.getCompressionInfo());
        if (!Arrays.equals(CompressionUtils.decompressIfNeeded(ordered.build()), data(i))) throw new AssertionError("ordered bytes");
        chunk.addKeyOrdered(key).addValueUnordered(value);
        if (chunk.getKeyOrderedCount() == 3 || n == batch.getKeyOrderedCount() - 1) {
          observer.onNext(chunk.build()); chunk.clear();
        }
      }
      observer.onCompleted();
      var entries = future.get();
      if (entries.size() != batch.getKeyOrderedCount()) throw new AssertionError("map count");
      int n = 0;
      for (var entry : entries.entrySet()) {
        int i = reverse ? batch.getKeyOrderedCount() - 1 - n : n;
        if (entry.getKey().getOrder() != i + 1L || !Arrays.equals(entry.getKey().getValue(), data(i)) || !Arrays.equals(entry.getValue().getValue(), data(i)))
          throw new AssertionError("ordered map bytes/order at " + i + " reverse=" + reverse);
        n++;
      }
    }
    var failed = new CompletableFuture<Map<OrderedPayload, Payload>>();
    var observer = new StreamBatchOrderedMapObserver(failed);
    observer.onNext(BatchValueResponse.newBuilder().addKeyOrdered(batch.getKeyOrdered(0)).build());
    if (!failed.isCompletedExceptionally()) throw new AssertionError("unpaired map accepted");
  }
  public static void main(String[] args) throws Exception {
    Path file = Path.of(args[1]);
    if (args[0].equals("write")) {
      var batch = produce(); verify(batch);
      Files.createDirectories(file.toAbsolutePath().getParent());
      Files.write(file, batch.toByteArray());
    } else {
      verify(BatchValueResponse.parseFrom(Files.readAllBytes(file)));
    }
    System.out.println("Java " + System.getProperty("hurricache.compression") + " offline " + args[0] + " passed");
  }
}
