package control

import (
	"time"
)

// This just contains the different opcodes

const (
	StatusUpdateOpCode   uint8 = 1
	UpdateIntervalOpCode uint8 = 2
)

type Duration time.Duration

func (d Duration) MarshalBinary() (data []byte, err error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *Duration) UnmarshalBinary(data []byte) error {
	d1, err := time.ParseDuration(string(data))
	*d = Duration(d1)
	return err
}
