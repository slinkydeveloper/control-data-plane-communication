package control

import (
	"time"
)

// This just contains the different opcodes

const (
	StatusUpdateOpCode   uint8 = 1
	UpdateIntervalOpCode uint8 = 2
)

func SerializeInterval(duration time.Duration) []byte {
	return []byte(duration.String())
}

func DeserializeInterval(payload []byte) (time.Duration, error) {
	return time.ParseDuration(string(payload))
}
