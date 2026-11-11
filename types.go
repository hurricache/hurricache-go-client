// Package hurricache provides concurrent direct and coordinator-routed HurriCache clients.
// Every operation accepts a context and returns decoded data with protocol metadata.
package hurricache

import (
	"bytes"
	"context"
	pb "github.com/hurricache/hurricache-go-client/proto/cachepb"
	"time"
)

// Ptr represents an explicitly supplied optional value, including zero.
func Ptr[T any](v T) *T { return &v }

// KeyHint contains server hash hints. Relay metadata does not affect Equal.
// A nil field is absent; a pointer to zero is an explicitly supplied zero hash.
type KeyHint struct {
	WeakHash, StrongHash *uint32
	WasRelayed           bool
}

func (h *KeyHint) Equal(other *KeyHint) bool {
	if h == nil || other == nil {
		return h == other
	}
	equal := func(a, b *uint32) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }
	return equal(h.WeakHash, other.WeakHash) && equal(h.StrongHash, other.StrongHash)
}

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

// Payload carries a byte map key and optional key metadata. Scalar and collection
// values use Variant; ordered map keys use OrderedPayload.
type Payload struct {
	Data            []byte
	ExpiresAtMillis *uint64
	Lock            *LockInfo
	Hint            *KeyHint
	Compression     *CompressionInfo
	ClientID        *uint32
}

// Equal compares only the Java byte-key identity, excluding transport metadata.
func (p Payload) Equal(other Payload) bool {
	return (p.Data == nil) == (other.Data == nil) && bytes.Equal(p.Data, other.Data)
}

// OrderedPayload adds an optional unsigned ordering weight to a payload.
type OrderedPayload struct {
	Payload
	Order *uint64
}

func (p OrderedPayload) Equal(other OrderedPayload) bool {
	return p.Payload.Equal(other.Payload) && (p.Order == nil && other.Order == nil || p.Order != nil && other.Order != nil && *p.Order == *other.Order)
}

// Entry pairs a binary map key with its decoded value.
type Entry struct {
	Key   Payload
	Value Variant
}

// OrderedEntry preserves both the binary map key and its ordering weight.
type OrderedEntry struct {
	Key   OrderedPayload
	Value Variant
}

// ValueResult preserves unordered/ordered response presence and the server hint.
type ValueResult struct {
	Value           *Variant
	Ordered         *OrderedVariant
	Hint            *KeyHint
	ExpiresAtMillis *uint64
	Lock            *LockInfo
	Compression     *CompressionInfo
}

// CASResult preserves the comparison result and optional observed value and hint.
type CASResult struct {
	Swapped  bool
	Expected *Variant
	Hint     *KeyHint
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
	Start uint64
	// OmitStart leaves pos absent, for an open lower range bound.
	OmitStart bool
	End       *uint64
	Type      *ContainerType
	Reverse   bool
}

// ContainerData supplies exactly one collection representation appropriate to Type.
type ContainerData struct {
	Values         []Variant
	OrderedValues  []OrderedVariant
	Entries        []Entry
	OrderedEntries []OrderedEntry
}

// Removal explicitly separates values from keys, avoiding Java's swapped overloads.
type Removal struct {
	Type   ContainerType
	Values []Variant
	Keys   []Payload
}

// ConnectionFactory permits custom TLS/dialing and borrowed connection ownership.
// Owned connections are closed by SmartClient. Borrowed connections are never closed.
type ConnectionFactory func(context.Context, string) (Connection, error)
