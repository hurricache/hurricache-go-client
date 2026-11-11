package hurricache

import (
	"fmt"
	"math"
)

// Version identifies this breaking API and protocol migration.
const Version = "0.2.0"

// VariantType is the exact stored type; NONE differs from empty bytes and zero.
type VariantType uint8

const (
	NoneType VariantType = iota
	BytesType
	IntType
	DoubleType
)

// Variant is an immutable, comparable value, usable as a Go map key. Bytes are
// copied on input and output. All NaNs compare equal, and signed zeros differ,
// matching Java Double.equals. Its zero value is NONE.
type Variant struct {
	kind VariantType
	data string
	bits uint64
}

func None() Variant               { return Variant{} }
func Bytes(value []byte) Variant  { return Variant{kind: BytesType, data: string(value)} }
func String(value string) Variant { return Variant{kind: BytesType, data: value} }
func Int(value int64) Variant     { return Variant{kind: IntType, bits: uint64(value)} }
func Double(value float64) Variant {
	bits := math.Float64bits(value)
	if math.IsNaN(value) {
		bits = 0x7ff8000000000000
	}
	return Variant{kind: DoubleType, bits: bits}
}
func (v Variant) Type() VariantType { return v.kind }
func (v Variant) IsNone() bool      { return v.kind == NoneType }
func (v Variant) IsBytes() bool     { return v.kind == BytesType }
func (v Variant) IsInt() bool       { return v.kind == IntType }
func (v Variant) IsDouble() bool    { return v.kind == DoubleType }
func (v Variant) IsNumber() bool    { return v.IsInt() || v.IsDouble() }
func (v Variant) Bytes() ([]byte, error) {
	if !v.IsBytes() {
		return nil, fmt.Errorf("hurricache: expected BYTES, got %v", v.kind)
	}
	return []byte(v.data), nil
}

func (v Variant) String() string {
	switch v.kind {
	case NoneType:
		return "Variant{none}"
	case BytesType:
		return fmt.Sprintf("Variant{bytes=%dB}", len(v.data))
	case IntType:
		return fmt.Sprintf("Variant{int=%d}", int64(v.bits))
	case DoubleType:
		return fmt.Sprintf("Variant{double=%v}", math.Float64frombits(v.bits))
	default:
		return "Variant{unknown}"
	}
}
func (v Variant) Int64() (int64, error) {
	if !v.IsInt() {
		return 0, fmt.Errorf("hurricache: expected INT, got %v", v.kind)
	}
	return int64(v.bits), nil
}
func (v Variant) Float64() (float64, error) {
	if !v.IsDouble() {
		return 0, fmt.Errorf("hurricache: expected DOUBLE, got %v", v.kind)
	}
	return math.Float64frombits(v.bits), nil
}

// Number returns an int64 or float64 without converting the observed numeric type.
func (v Variant) Number() (any, error) {
	if v.IsInt() {
		return int64(v.bits), nil
	}
	if v.IsDouble() {
		return math.Float64frombits(v.bits), nil
	}
	return nil, fmt.Errorf("hurricache: expected numeric value, got %v", v.kind)
}

// OrderedVariant is immutable and comparable, including optional order presence.
// A nil order remains absent on reads; writes use order zero as in Java.
type orderedValue = Variant

type OrderedVariant struct {
	orderedValue
	order    uint64
	hasOrder bool
}

func Ordered(value Variant, order *uint64) OrderedVariant {
	v := OrderedVariant{orderedValue: value}
	if order != nil {
		v.order, v.hasOrder = *order, true
	}
	return v
}
func (v OrderedVariant) Order() *uint64 {
	if !v.hasOrder {
		return nil
	}
	return Ptr(v.order)
}
func (v OrderedVariant) Value() Variant { return v.orderedValue }
func (v OrderedVariant) String() string {
	if !v.hasOrder {
		return fmt.Sprintf("OrderedVariant{value=%v, order=null}", v.Value())
	}
	return fmt.Sprintf("OrderedVariant{value=%v, order=%d}", v.Value(), v.order)
}
