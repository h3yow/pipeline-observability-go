package event

// decoders maps schema version -> decode function.
var decoders = map[int]func(*Envelope, *DecodedEvent) error{}
