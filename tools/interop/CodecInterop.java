import com.hurricache.client.intf.OrderedPayload;
import com.hurricache.client.intf.Payload;
import com.hurricache.client.intf.Variant;
import com.hurricache.client.KeyValueUtils;
import com.hurricache.grpc.BatchValueResponse;
import com.hurricache.grpc.Value;
import com.hurricache.grpc.OrderedValue;
import com.hurricache.grpc.ValueResponse;
import com.hurricache.grpc.AtomicCasRes;
import com.hurricache.grpc.KeyHint;
import com.hurricache.grpc.BatchCommand;
import com.hurricache.grpc.BatchMessage;
import com.hurricache.grpc.BatchSingleMessage;
import com.hurricache.grpc.ReplicationCommandType;
import com.hurricache.grpc.UpdateContainerRequest;
import com.hurricache.utils.CompressionUtils;
import com.hurricache.utils.StreamBatchOrderedMapObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
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
    static void updates(String action, Path base) throws Exception {
        Path fixture = Path.of(base + ".updates");
        if (action.equals("write")) {
            var absolute = UpdateContainerRequest.newBuilder()
                .setKey(KeyValueUtils.buildKey("container".getBytes(), null, 17, 1024))
                .setOrderedElementKey(KeyValueUtils.createOrderedKey(typedData(4096), 10, 17, 1024))
                .setNewOrder(0).build();
            var delta = absolute.toBuilder().setDeltaOrder(-5).build();
            var plain = UpdateContainerRequest.newBuilder()
                .setKey(absolute.getKey())
                .setElementKey(KeyValueUtils.createUnorderedKey("entry".getBytes(), 17, null))
                .setValue(KeyValueUtils.buildValueFromVariantCompressed(Variant.ofLong(0), 1024)).build();
            BatchMessage.Builder batch = BatchMessage.newBuilder();
            for (var update : new UpdateContainerRequest[]{absolute, delta, plain}) {
                batch.addBatchSingleMessages(BatchSingleMessage.newBuilder()
                    .setReplicationCommandType(ReplicationCommandType.UPDATE_IN_CONTAINER_BY_KEY)
                    .setBatchCommand(BatchCommand.newBuilder().setUpdateInContainerByKey(update)));
            }
            Files.write(fixture, batch.build().toByteArray());
        } else {
            var batch = BatchMessage.parseFrom(Files.readAllBytes(fixture));
            if (batch.getBatchSingleMessagesCount() != 3) throw new AssertionError("update count");
            for (int index=0; index<3; index++) {
                var update = batch.getBatchSingleMessages(index).getBatchCommand().getUpdateInContainerByKey();
                if (index<2) {
                    if (!update.hasOrderedElementKey() || update.hasValue() || update.getOrderedElementKey().getOrder()!=10
                        || update.getOrderedElementKey().getClientId()!=17
                        || !Arrays.equals(CompressionUtils.decompressIfNeeded(update.getOrderedElementKey()),typedData(4096)))
                        throw new AssertionError("ordered update selector");
                    if (index==0 && (!update.hasNewOrder() || update.getNewOrder()!=0)) throw new AssertionError("absolute zero order");
                    if (index==1 && (!update.hasDeltaOrder() || update.getDeltaOrder()!=-5)) throw new AssertionError("signed order delta");
                } else if (!update.hasElementKey() || !update.hasValue()
                    || !KeyValueUtils.toVariant(ValueResponse.newBuilder().setValueUnordered(update.getValue()).build()).equals(Variant.ofLong(0)))
                    throw new AssertionError("plain value field migration");
            }
        }
    }
    static Variant[] mixed() {
        return new Variant[]{Variant.none(), Variant.ofBytes(new byte[0]), Variant.ofLong(0), Variant.ofDouble(0),
            Variant.ofLong(Long.MIN_VALUE), Variant.ofLong(Long.MAX_VALUE), Variant.ofDouble(-0.0),
            Variant.ofDouble(1.25), Variant.ofBytes(typedData(4096)), Variant.ofDouble(Double.NaN)};
    }

    static void typed(String action, Path base) throws Exception {
        Path fixture = Path.of(base + ".typed");
        if (action.equals("write")) {
            BatchValueResponse.Builder batch = BatchValueResponse.newBuilder();
            for (Variant variant : mixed()) batch.addValueUnordered(KeyValueUtils.buildValueFromVariantCompressed(variant, 1024));
            Files.write(fixture, batch.build().toByteArray());
            Files.write(Path.of(base + ".cas"), AtomicCasRes.newBuilder().setResult(false).setExpected(
                ValueResponse.newBuilder().setValueUnordered(KeyValueUtils.buildValueFromVariantCompressed(Variant.ofDouble(-0.0), 1024))
                .setKeyHint(KeyHint.newBuilder().setWeakHash(0))).build().toByteArray());
        } else {
            BatchValueResponse batch = BatchValueResponse.parseFrom(Files.readAllBytes(fixture));
            if (batch.getValueUnorderedCount() != mixed().length) throw new AssertionError("typed count");
            int index = 0;
            for (Value value : batch.getValueUnorderedList()) {
                Variant actual = KeyValueUtils.toVariant(ValueResponse.newBuilder().setValueUnordered(value).build());
                if (!actual.equals(mixed()[index++])) throw new AssertionError("typed value " + index + ": " + actual);
            }
            AtomicCasRes cas = AtomicCasRes.parseFrom(Files.readAllBytes(Path.of(base + ".cas")));
            if (!cas.hasExpected() || !KeyValueUtils.toVariant(cas.getExpected()).equals(Variant.ofDouble(-0.0))
                || !cas.getExpected().getKeyHint().hasWeakHash()) throw new AssertionError("nested CAS");
        }
    }
    static byte[] typedData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) data[i] = (byte) (i % 251);
        return data;
    }


  public static void main(String[] args) throws Exception {
    Path file = Path.of(args[1]);
    typed(args[0],file);updates(args[0],file);
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
