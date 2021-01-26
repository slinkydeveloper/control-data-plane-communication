package samplesource

import (
	"fmt"
	"time"
)

// This just contains the different opcodes

const (
	UpdateActiveStatusOpCode uint8 = 1
	UpdateIntervalOpCode     uint8 = 2
	NotifyActiveStatusOpCode uint8 = 3
	NotifyIntervalOpCode     uint8 = 4
)

type ActiveStatus bool

func (as ActiveStatus) IsRunning() bool {
	return bool(as)
}

func (as ActiveStatus) IsPaused() bool {
	return !bool(as)
}

func (as ActiveStatus) MarshalBinary() (data []byte, err error) {
	if as {
		return []byte{0x00}, err
	} else {
		return []byte{0xFF}, err
	}
}

func (as *ActiveStatus) UnmarshalBinary(data []byte) error {
	if len(data) != 1 {
		return fmt.Errorf("wrong data length: %d != 1", len(data))
	}
	switch data[0] {
	case 0xFF:
		*as = false
		return nil
	case 0x00:
		*as = true
		return nil
	default:
		return fmt.Errorf("unexpected value %X, allowed only 0xFF or 0x00", data[0])
	}
}

func ActiveStatusParser(b []byte) (interface{}, error) {
	var s ActiveStatus
	if err := s.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return &s, nil
}

type Duration time.Duration

func (d Duration) MarshalBinary() (data []byte, err error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *Duration) UnmarshalBinary(data []byte) error {
	d1, err := time.ParseDuration(string(data))
	*d = Duration(d1)
	return err
}

func DurationParser(b []byte) (interface{}, error) {
	var d Duration
	if err := d.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return &d, nil
}
