# Go API reference

Version **0.2.0**; Java `0a2b1d851405744e9390bcf5b5e8592177b121e4`.

Client and SmartClient implement the identical Operations interface below. Calls accept context.Context, a byte key and Options; use []byte(text) for UTF-8 strings. Goroutines provide concurrency.
Nil option pointers inherit Config defaults. Options.ClientID=Ptr(uint32(0)) retains explicit zero; Timeout=Ptr(time.Duration(0)) disables the client timeout while respecting caller cancellation. Config defaults: ID 0, timeout 1 second, TTL 0, GZIP, compression threshold 1024 bytes, maximum request 3.5 MiB, inbound/decoded limit 64 MiB. TLS and DialOptions configure transport independently of payload compression.
Options.Mode overrides each operation's Java dispatch policy; nil uses that policy and SmartConfig.Mode. There is no mutable goroutine-local mode. SmartClient adds Ready(context.Context), IsReady() and Refresh(context.Context); owned node/coordinator connections close only when no active operation uses them. NewClientWithConnection borrows its connection.

## Typed values

None(), Bytes([]byte), String(string), Int(int64), Double(float64) create immutable comparable Variant values. Zero Variant is NONE. Type(), IsNone(), IsBytes(), IsInt(), IsDouble(), IsNumber() inspect type. Bytes(), Int64(), Float64(), Number() return (value,error), rejecting wrong types. Number() contains int64 or float64. Bytes copy caller input/output; NaNs compare equal and signed double zeros differ. Variant is safe as a Go map key.
Ordered(Variant,*uint64) creates a comparable OrderedVariant; Value() returns its Variant and Order() returns a copy of its optional order. Variant methods are promoted. Missing order remains nil on reads and encodes as zero on writes.
Payload{Data: []byte(...)} and OrderedPayload{Payload: ..., Order: Ptr(uint64(...))} represent byte map keys. Entry and OrderedEntry pair those keys with Variant values; slices preserve stream order and duplicate byte keys at distinct orders. ContainerData contains exactly one of Values, OrderedValues, Entries, OrderedEntries.

## Operations

```go
type Operations interface {
    Close() error
    Target() string
    DefaultClientID() uint32
    DefaultTimeout() time.Duration
    DefaultTTL() time.Duration
    DefaultCompressionThreshold() int
    Get(ctx context.Context, key []byte, o Options) (Variant, error)
    Pop(ctx context.Context, key []byte, o Options) (Variant, error)
    PeekHead(ctx context.Context, key []byte, o Options) (Variant, error)
    PeekTail(ctx context.Context, key []byte, o Options) (Variant, error)
    PopHead(ctx context.Context, key []byte, o Options) (Variant, error)
    PopTail(ctx context.Context, key []byte, o Options) (Variant, error)
    ContainsKey(ctx context.Context, key []byte, o Options) (bool, error)
    Remove(ctx context.Context, key []byte, o Options) (bool, error)
    GetSize(ctx context.Context, key []byte, o Options) (uint32, error)
    DeleteHead(ctx context.Context, key []byte, o Options) (uint32, error)
    DeleteTail(ctx context.Context, key []byte, o Options) (uint32, error)
    Create(ctx context.Context, key []byte, value Variant, o Options) (*KeyHint, error)
    Replace(ctx context.Context, key []byte, value Variant, o Options) (Variant, error)
    GetAt(ctx context.Context, key []byte, pos uint64, o Options) (Variant, error)
    PopAt(ctx context.Context, key []byte, pos uint64, o Options) (Variant, error)
    RemoveAt(ctx context.Context, key []byte, pos Position, o Options) (uint32, error)
    GetElementWithWeight(ctx context.Context, key []byte, pos uint64, o Options) (Variant, error)
    GetAndRemoveElementWithWeight(ctx context.Context, key []byte, pos uint64, o Options) (Variant, error)
    GetInContainer(ctx context.Context, key, elementKey []byte, o Options) (Variant, error)
    PopInContainer(ctx context.Context, key, elementKey []byte, o Options) (Variant, error)
    ContainsInContainer(ctx context.Context, key, elementKey []byte, o Options) (bool, error)
    RemoveInContainer(ctx context.Context, key, elementKey []byte, o Options) (uint32, error)
    Update(ctx context.Context, key, elementKey []byte, value Variant, o Options) (Variant, error)
    UpdateOrder(ctx context.Context, key []byte, elementKey OrderedPayload, newOrder uint64, o Options) (bool, error)
    IncrementOrder(ctx context.Context, key []byte, elementKey OrderedPayload, delta int64, o Options) (bool, error)
    Exchange(ctx context.Context, key []byte, value any, o Options) (any, error)
    ExchangeInContainer(ctx context.Context, key []byte, selector any, value any, o Options) (any, error)
    Add(ctx context.Context, key []byte, value any, o Options) (any, error)
    AddInContainer(ctx context.Context, key []byte, selector any, value any, o Options) (any, error)
    Sub(ctx context.Context, key []byte, value any, o Options) (any, error)
    SubInContainer(ctx context.Context, key []byte, selector any, value any, o Options) (any, error)
    And(ctx context.Context, key []byte, value int64, o Options) (int64, error)
    AndInContainer(ctx context.Context, key []byte, selector any, value int64, o Options) (int64, error)
    Or(ctx context.Context, key []byte, value int64, o Options) (int64, error)
    OrInContainer(ctx context.Context, key []byte, selector any, value int64, o Options) (int64, error)
    Xor(ctx context.Context, key []byte, value int64, o Options) (int64, error)
    XorInContainer(ctx context.Context, key []byte, selector any, value int64, o Options) (int64, error)
    CompareAndSet(ctx context.Context, key []byte, expected, newValue any, o Options) (CASResult, error)
    CompareAndSetInContainer(ctx context.Context, key []byte, selector any, expected, newValue any, o Options) (CASResult, error)
    SetTTL(ctx context.Context, key []byte, ttl time.Duration, o Options) (bool, error)
    GetTTL(ctx context.Context, key []byte, o Options) (int64, error)
    LockObject(ctx context.Context, key []byte, kind LockType, duration time.Duration, o Options) (LockResult, error)
    UnlockObject(ctx context.Context, key []byte, o Options) (LockResult, error)
    GetValue(ctx context.Context, key []byte, o Options) (Variant, error)
    GetAndDeleteValue(ctx context.Context, key []byte, o Options) (Variant, error)
    ExistKey(ctx context.Context, key []byte, o Options) (bool, error)
    GetFront(ctx context.Context, key []byte, o Options) (Variant, error)
    GetHead(ctx context.Context, key []byte, o Options) (Variant, error)
    GetTail(ctx context.Context, key []byte, o Options) (Variant, error)
    GetAndRemoveFront(ctx context.Context, key []byte, o Options) (Variant, error)
    GetAndRemoveTail(ctx context.Context, key []byte, o Options) (Variant, error)
    RemoveHead(ctx context.Context, key []byte, o Options) (uint32, error)
    RemoveTail(ctx context.Context, key []byte, o Options) (uint32, error)
    CreateKeyValue(ctx context.Context, key, value []byte, o Options) (*KeyHint, error)
    UpdateKeyValue(ctx context.Context, key, value []byte, o Options) (Variant, error)
    GetContainerValue(ctx context.Context, key, elementKey []byte, o Options) (Variant, error)
    GetAndRemoveContainerValue(ctx context.Context, key, elementKey []byte, o Options) (Variant, error)
    ContainsContainerKey(ctx context.Context, key, elementKey []byte, o Options) (bool, error)
    RemoveContainerKey(ctx context.Context, key, elementKey []byte, o Options) (uint32, error)
    UpdateContainerValue(ctx context.Context, key, elementKey, value []byte, o Options) (Variant, error)
    Lock(ctx context.Context, key []byte, kind LockType, duration time.Duration, o Options) (LockStatus, error)
    Unlock(ctx context.Context, key []byte, o Options) (LockStatus, error)
    CreateContainer(ctx context.Context, key []byte, t ContainerType, data ContainerData, o Options) (*KeyHint, error)
    RemoveFromContainer(ctx context.Context, key []byte, removal Removal, o Options) (uint32, error)
    GetContainer(ctx context.Context, key []byte, o Options) (ContainerData, error)
    GetElementInRange(ctx context.Context, key []byte, pos Position, o Options) (ContainerData, error)
    CreateQueue(ctx context.Context, key []byte, data []Variant, o Options) (*KeyHint, error)
    GetQueue(ctx context.Context, key []byte, o Options) ([]Variant, error)
    StreamQueue(ctx context.Context, key []byte, o Options) ([]Variant, error)
    CreateList(ctx context.Context, key []byte, data []Variant, o Options) (*KeyHint, error)
    GetList(ctx context.Context, key []byte, o Options) ([]Variant, error)
    StreamList(ctx context.Context, key []byte, o Options) ([]Variant, error)
    CreateVector(ctx context.Context, key []byte, data []Variant, o Options) (*KeyHint, error)
    GetVector(ctx context.Context, key []byte, o Options) ([]Variant, error)
    StreamVector(ctx context.Context, key []byte, o Options) ([]Variant, error)
    CreateSet(ctx context.Context, key []byte, data []Variant, o Options) (*KeyHint, error)
    GetSet(ctx context.Context, key []byte, o Options) ([]Variant, error)
    StreamSet(ctx context.Context, key []byte, o Options) ([]Variant, error)
    CreateOrderedSet(ctx context.Context, key []byte, data []OrderedVariant, o Options) (*KeyHint, error)
    GetOrderedSet(ctx context.Context, key []byte, o Options) ([]OrderedVariant, error)
    StreamOrderedSet(ctx context.Context, key []byte, o Options) ([]OrderedVariant, error)
    CreateMap(ctx context.Context, key []byte, data []Entry, o Options) (*KeyHint, error)
    GetMap(ctx context.Context, key []byte, o Options) ([]Entry, error)
    StreamMap(ctx context.Context, key []byte, o Options) ([]Entry, error)
    CreateOrderedMap(ctx context.Context, key []byte, data []OrderedEntry, o Options) (*KeyHint, error)
    GetOrderedMap(ctx context.Context, key []byte, o Options) ([]OrderedEntry, error)
    StreamOrderedMap(ctx context.Context, key []byte, o Options) ([]OrderedEntry, error)
    PushHead(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error)
    PushTail(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error)
    AddElementUnordered(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error)
    AddElementWithWeight(ctx context.Context, key []byte, data []OrderedVariant, o Options) (uint32, error)
    InsertAt(ctx context.Context, key []byte, data []Variant, pos uint32, o Options) (uint32, error)
    InsertBefore(ctx context.Context, key []byte, data []Variant, pivot Variant, o Options) (uint32, error)
    InsertAfter(ctx context.Context, key []byte, data []Variant, pivot Variant, o Options) (uint32, error)
    PutAll(ctx context.Context, key []byte, data []Entry, o Options) (uint32, error)
    AddElementOrderedMap(ctx context.Context, key []byte, data []OrderedEntry, o Options) (uint32, error)
    GetRange(ctx context.Context, key []byte, pos Position, o Options) ([]Variant, error)
    PopRange(ctx context.Context, key []byte, pos Position, o Options) (ContainerData, error)
    GetListElementsInRange(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]Variant, error)
    GetAndDeleteListElementsInRange(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]Variant, error)
    GetVectorElementsInRange(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]Variant, error)
    GetAndDeleteVectorElementsInRange(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]Variant, error)
    GetElementInRangeOrderedSet(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]OrderedVariant, error)
    GetAndDeleteElementInRangeOrderedSet(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]OrderedVariant, error)
    GetElementInRangeOrderedMap(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]OrderedEntry, error)
    GetAndDeleteElementInRangeOrderedMap(ctx context.Context, key []byte, start, end *uint64, reverse bool, o Options) ([]OrderedEntry, error)
    RemoveFromContainerByKey(ctx context.Context, key []byte, containerKey Payload, o Options) (uint32, error)
    RemoveFromContainerByKeyAndValue(ctx context.Context, key []byte, keys []Payload, values []Variant, o Options) (uint32, error)
    AddElement(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error)
    AddElementOrdered(ctx context.Context, key []byte, data []OrderedVariant, o Options) (uint32, error)
    AddElementOrderedSet(ctx context.Context, key []byte, data []OrderedVariant, o Options) (uint32, error)
    AddElementToHead(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error)
    AddElementToHeadCount(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error)
    AddElementToTail(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error)
    AddElementToTailCount(ctx context.Context, key []byte, data []Variant, o Options) (uint32, error)
    AddElementToPosition(ctx context.Context, key []byte, data []Variant, pos uint32, o Options) (uint32, error)
    AddElementToPositionBefore(ctx context.Context, key []byte, data []Variant, pivot Variant, o Options) (uint32, error)
    AddElementToPositionBeforeCount(ctx context.Context, key []byte, data []Variant, pivot Variant, o Options) (uint32, error)
    AddElementToPositionAfter(ctx context.Context, key []byte, data []Variant, pivot Variant, o Options) (uint32, error)
    AddElementToPositionAfterCount(ctx context.Context, key []byte, data []Variant, pivot Variant, o Options) (uint32, error)
    AddElementHashMap(ctx context.Context, key []byte, data []Entry, o Options) (uint32, error)
    StreamElementInRangeUnordered(ctx context.Context, key []byte, t ContainerType, start, end uint32, o Options) ([]Variant, error)
    StreamElementInRangeOrderedSet(ctx context.Context, key []byte, start, end uint64, reverse bool, o Options) ([]OrderedVariant, error)
    StreamElementInRangeOrdered(ctx context.Context, key []byte, start, end uint64, o Options) ([]OrderedVariant, error)
    StreamElementInRangeOrderedMap(ctx context.Context, key []byte, start, end uint64, reverse bool, o Options) ([]OrderedEntry, error)
    AtomicExchange(ctx context.Context, key []byte, value int64, o Options) (any, error)
    AtomicAdd(ctx context.Context, key []byte, value int64, o Options) (any, error)
    AtomicSub(ctx context.Context, key []byte, value int64, o Options) (any, error)
    AtomicAnd(ctx context.Context, key []byte, value int64, o Options) (int64, error)
    AtomicOr(ctx context.Context, key []byte, value int64, o Options) (int64, error)
    AtomicXor(ctx context.Context, key []byte, value int64, o Options) (int64, error)
    AtomicCompareAndSet(ctx context.Context, key []byte, expected, newValue int64, o Options) (CASResult, error)
    GetElementAtPosition(ctx context.Context, key []byte, pos Position, o Options) (Variant, error)
    GetAndRemoveElementAtPosition(ctx context.Context, key []byte, pos Position, o Options) (Variant, error)
    RemoveElementAtPosition(ctx context.Context, key []byte, pos Position, o Options) (uint32, error)
}
```

Create/Get/Replace/Pop use Variant directly. GetOrderedSet and weighted set ranges return []OrderedVariant. Exchange/Add/Sub and their container versions accept int, int64 or float64 and return an int64 or float64 inside any; bitwise calls accept/return int64. Bool operands/selectors are rejected. Container selectors accept []byte/string entry keys or nonnegative int/int64/uint64 positions; exactly one protobuf alternative is set. CASResult.Expected is an optional Variant and Hint comes from the nested response; either double operand promotes both expected/new operands to double.
Mutation counts use uint32, including zero on empty insertion without an RPC. Update returns the previous map-entry Variant. UpdateOrder/IncrementOrder require an OrderedPayload with the exact current order; the server returns bool and enforces arithmetic bounds while preserving the member's value.
Head chunks stay in input order to preserve the server's per-request reversal across batches. Pivot insertions advance the anchor after each chunk. Every serialized request, including the full anchor and continuation hints, is checked before mutation.
CompareAndSetInContainer selects a map field with []byte/string or an exact ordered-map field with OrderedPayload carrying Order. It uses write routing and leaves element TTL absent, matching Java. Positions/bools are rejected. GetQueue/StreamQueue are legacy conveniences unsupported by the server contract; use GetSize, PeekHead/PeekTail and PopHead/PopTail for queues.
Position.Start/End encode unsigned range bounds; OmitStart and nil End represent absence. Destructive ranges omit zero bounds as in Java; named range methods accept *uint64 bounds. RemoveAt sends End only when it exceeds Start. SetTTL uses relative time.Duration; negative clears expiration, zero expires now; GetTTL returns remaining milliseconds or -1. Lock duration encodes absolute Unix seconds, matching Java despite the schema comment. Options.ClientID owns lock/unlock calls.
Every operation has one effective deadline across preprocessing, discovery, all chunks, streaming and one permitted routing fallback. RPCError retains gRPC status and trailers. PartialError reports acknowledged chunks/items; a stream failure returns the decoded prefix and prohibits whole-operation replay. Received destructive batches count as progress even if local decoding fails.

## Lifecycle adapters

NewJedis(Operations) and NewJedisPool(Operations) wrap an existing direct/smart client. Jedis.Ping returns local PONG. GetResource returns a new wrapper sharing the client. Closing a resource or pool closes that client. No commented Java Redis commands or application replication API are implemented.

Deprecated names remain only where argument binding is unambiguous and use current typed/native results. AtomicCreate/AtomicStore/AtomicLoad/AtomicLoadAndDelete are removed: use Create/Replace/Get/Pop with Int or Double. See README.md for migration examples and API_PARITY.md for every Java overload, argument adaptation and routing policy.
