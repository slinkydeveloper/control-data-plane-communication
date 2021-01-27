package service

import (
	"context"

	"go.uber.org/zap"
	"knative.dev/pkg/logging"

	"knative.dev/control-data-plane-communication/pkg/control"
)

type messageRouter map[uint8]control.MessageHandler

func NewMessageRouter(routes map[uint8]control.MessageHandler) control.MessageHandler {
	return messageRouter(routes)
}

func (c messageRouter) HandleServiceMessage(ctx context.Context, message control.ServiceMessage) {
	logger := logging.FromContext(ctx)

	handler, ok := c[message.Headers().OpCode()]
	if ok {
		handler.HandleServiceMessage(ctx, message)
		return
	}

	message.Ack()
	logger.Warnw(
		"Received an unknown message, I don't know what to do with it",
		zap.Uint8("opcode", message.Headers().OpCode()),
		zap.ByteString("payload", message.Payload()),
	)
}
