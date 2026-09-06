// Package hurricache provides concurrent direct and coordinator-routed HurriCache clients.
// Every operation accepts a context and returns decoded data with protocol metadata.
package hurricache

import (
	"context"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"time"
)

// Ptr represents an explicitly supplied optional value, including zero.
func Ptr[T any](v T) *T { return &v }

// KeyHint contains server hash hints. WeakHash maps to the protocol's week_hash.
// A nil field is absent; a pointer to zero is an explicitly supplied zero hash.
type KeyHint struct{ WeakHash, StrongHash *uint32 }

// CompressionInfo describes the bytes as received, before transparent decoding.
type CompressionInfo struct {
	Enabled bool
	RawSize *uint32
}

// LockInfo preserves lock ownership and the server's absolute millisecond timestamp.
type LockInfo struct {
	Type             LockType
	LockedBy         *uint32
	LockedTillMillis *uint64
}

// Payload is a present binary value. A nil *Payload denotes an absent response.
// ExpiresAtMillis is the unmodified optional wire timestamp.
type Payload struct {
	Data            []byte
	ExpiresAtMillis *uint64
	Lock            *LockInfo
	Hint            *KeyHint
	Compression     *CompressionInfo
	ClientID        *uint32
}

// OrderedPayload adds an optional unsigned ordering weight to a payload.
type OrderedPayload struct {
	Payload
	Order *uint64
}

// Entry pairs a binary map key with its decoded value.
type Entry struct{ Key, Value Payload }

// OrderedEntry preserves both the binary map key and its ordering weight.
type OrderedEntry struct {
	Key   OrderedPayload
	Value Payload
}

// ValueResult preserves unordered/ordered response presence and the server hint.
type ValueResult struct {
	Value   *Payload
	Ordered *OrderedPayload
	Hint    *KeyHint
}

// UpdateResult preserves the operation result separately from the previous value.
type UpdateResult struct {
	Updated  bool
	Previous *Payload
}

// AtomicResult preserves the signed 64-bit value and optional server hint.
type AtomicResult struct {
	Value int64
	Hint  *KeyHint
}

// CASResult preserves the comparison result and optional observed value and hint.
type CASResult struct {
	Swapped  bool
	Expected *AtomicResult
	Hint     *KeyHint
}

// TTLResult distinguishes an absent expiration from an expiration at zero.
type TTLResult struct {
	Remaining       *time.Duration
	ExpiresAtMillis *uint64
}

// LockResult retains the server status and optional diagnostic.
type LockResult struct {
	Status  LockStatus
	Message *string
}

// ContainerType identifies the server data structure.
type ContainerType = pb.ContainerType

const (
	Undefined  = pb.ContainerType_UNDEFINED
	Vector     = pb.ContainerType_VECTOR
	List       = pb.ContainerType_LIST
	Queue      = pb.ContainerType_QUEUE
	Set        = pb.ContainerType_SET
	Map        = pb.ContainerType_MAP
	OrderedMap = pb.ContainerType_ORDERED_MAP
	OrderedSet = pb.ContainerType_ORDERED_SET
)

// LockType selects the protocol lock mode.
type LockType = pb.LockType

const (
	NoLock     = pb.LockType_NO_LOCK
	WriteLock  = pb.LockType_WRITE_LOCK
	ReadLock   = pb.LockType_READ_LOCK
	GlobalLock = pb.LockType_GLOBAL
)

// LockStatus is independent of transport success.
type LockStatus = pb.LockStatus

const (
	LockOK           = pb.LockStatus_OK
	CantLock         = pb.LockStatus_CANT_LOCK
	CantUnlock       = pb.LockStatus_CANT_UNLOCK
	LockGenericError = pb.LockStatus_GENERIC_ERROR
)

// RoutingMode selects a role and, where applicable, one unavailable-node fallback.
type RoutingMode uint8

const (
	MasterThenBackup RoutingMode = iota
	Master
	Backup
	LBSmart
)

// Options consolidates Java hint, client ID, TTL, timeout and routing overloads.
// Nil means inherit the client default. TTL zero omits expiration on creation/update.
// Timeout zero disables the default timeout; the caller's deadline still applies.
// Mode overrides even an operation's normal write dispatch policy.
type Options struct {
	Hint     *KeyHint
	ClientID *uint32
	TTL      *time.Duration
	Timeout  *time.Duration
	Mode     *RoutingMode
}

// Position describes a point or inclusive range; nil End means a single position.
type Position struct {
	Start   uint64
	End     *uint64
	Type    *ContainerType
	Reverse bool
}

// ContainerData supplies exactly one collection representation appropriate to Type.
type ContainerData struct {
	Values         []Payload
	OrderedValues  []OrderedPayload
	Entries        []Entry
	OrderedEntries []OrderedEntry
}

// Removal explicitly separates values from keys, avoiding Java's swapped overloads.
type Removal struct {
	Type   ContainerType
	Values []Payload
	Keys   []Payload
}

// ConnectionFactory permits custom TLS/dialing and borrowed connection ownership.
// Owned connections are closed by SmartClient. Borrowed connections are never closed.
type ConnectionFactory func(context.Context, string) (Connection, error)
