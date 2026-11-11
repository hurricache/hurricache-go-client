package hurricache

// dataOf keeps byte-oriented regression assertions readable after typed migration.
// It fails loudly for a numeric value; typed scenarios use checked accessors.
func dataOf(value any) []byte {
	switch v := value.(type) {
	case Payload:
		return v.Data
	case OrderedPayload:
		return v.Data
	case Variant:
		b, e := v.Bytes()
		if e != nil {
			panic(e)
		}
		return b
	case *Variant:
		b, e := v.Bytes()
		if e != nil {
			panic(e)
		}
		return b
	case OrderedVariant:
		b, e := v.Bytes()
		if e != nil {
			panic(e)
		}
		return b
	case *OrderedVariant:
		b, e := v.Bytes()
		if e != nil {
			panic(e)
		}
		return b
	default:
		panic("unexpected byte assertion type")
	}
}
