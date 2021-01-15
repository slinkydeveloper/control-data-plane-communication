package controlprotocol

import "time"

const (
	clientInitialDialRetry  = 5
	clientReconnectionRetry = 5
	clientDialRetryInterval = 100 * time.Millisecond

	maximumSupportedVersion uint16 = 0
	outboundMessageVersion         = maximumSupportedVersion

	writeRetries = 5

	keepAlive = 30 * time.Second

	serverWaitForReconn = 500 * time.Millisecond

	AckOpCode uint8 = 0
)
