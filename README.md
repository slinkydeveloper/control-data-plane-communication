# Control plane <-> Data plane commands protocol

The goal of this protocol is to experiment a unified way to enable the control plane to send commands to the data plane, and from the data plane send notifications to the control plane.

Thanks to this protocol we could:

* Develop MT sources with ease, partitioning the workload and communicating it using this protocol
* Implement features that require cooperation between data-plane and control-plane, without mutating CRDs (e.g. status update notification from data plane, pause/resume, ...)
* Completely decouple data-plane components from Kubernetes

This protocol guarantees:

* Bi-directional streaming of commands
* At least once with acks
* Automatic reconnection on broken pipes
* AuthN/AuthZ with mutual TLS for controllers deploying the data-plane pods

This example shows how this protocol is used to mutate the interval used to send events of the sample source. 
The _netcode_ is available at [`pkg/controlprotocol`](pkg/control/network), 
the RA code is available at [`pkg/adapter/adapter.go`](pkg/adapter/adapter.go) and 
the controller code to trigger the interval update is available in the function [`UpdateInterval`](pkg/reconciler/sample/samplesource.go).

## Build, run and test

This example shows how this protocol is used to mutate the interval used to send events of the sample source.

To build and deploy to Kubernetes, run:

```shell
ko apply -f config
```

To create the source instance:

```shell
kubectl apply -f example.yaml
```

Now look at the logs of the `knative-samples` namespace:

```shell
stern -n knative-samples .
```

Try to modify the interval in `example.yaml` of the source, reapply `example.yaml` and look how the heartbeat message frequency changes in the logs.

## Pause resume example

Controller sending pause/resume command:

```go
ctrl, err := controlprotocol.ControlInterfaceFromContext(ctx, "control-plane", raIp)
if err != nil {
    logging.FromContext(ctx).Fatalw("Cannot start the control server", zap.Error(err))
}
defer ctrl.Close(ctx)

go func() {
    for ctrlMessage := range ctrl.InboundMessages() {
        ctrlMessage.Ack()

        // Status update here
        logging.FromContext(ctx).Infof("Received message: %v", ctrlMessage.Event())
    }
}()

// Let's send some pause/resume messages
ev := cloudevents.NewEvent()
ev.SetID(uuid.New().String())
ev.SetType("pause")

err = ctrl.SendAndWaitForAck(ev)
if err != nil {
    logging.FromContext(ctx).Fatalw("Error while sending the event", zap.Error(err))
}

// Let's wait for the notification event to be printed
time.Sleep(10 * time.Second)

ev = cloudevents.NewEvent()
ev.SetID(uuid.New().String())
ev.SetType("resume")

err = ctrl.SendAndWaitForAck(ev)
if err != nil {
    logging.FromContext(ctx).Fatalw("Error while sending the event", zap.Error(err))
}

// Let's wait for the notification event to be printed
time.Sleep(10 * time.Second)
```

Receive Adapter sample:

```go
ctrl, err := controlprotocol.StartControlServer(ctx, "data-plane")
if err != nil {
    logging.FromContext(ctx).Fatalw("Cannot start the control server", zap.Error(err))
}

for ctrlMessage := range ctrl.InboundMessages() {
    ctrlMessage.Ack()
    logging.FromContext(ctx).Infof("Received message: %v", ctrlMessage.Event())

    time.Sleep(1 * time.Second)

    ev := cloudevents.NewEvent()
    if ctrlMessage.Event().Type() == "pause" {
    	// Pause the component
        ev.SetID(uuid.New().String())
        ev.SetType("paused")
    } else if ctrlMessage.Event().Type() == "resume" {
        // Resume the component
        ev.SetID(uuid.New().String())
        ev.SetType("resumed")
    }

    // Notify operation done
    err = ctrl.SendAndWaitForAck(ev)
    if err != nil {
        logging.FromContext(ctx).Fatalw("Error while sending the event", zap.Error(err))
    }
}
```
