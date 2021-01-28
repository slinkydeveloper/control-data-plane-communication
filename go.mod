module knative.dev/control-data-plane-communication

go 1.14

require (
	github.com/cloudevents/sdk-go/v2 v2.2.0
	github.com/google/go-cmp v0.5.4
	github.com/google/uuid v1.1.2
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/stretchr/testify v1.6.1
	go.uber.org/atomic v1.7.0
	go.uber.org/zap v1.16.0
	k8s.io/api v0.19.7
	k8s.io/apimachinery v0.19.7
	k8s.io/client-go v0.19.7
	k8s.io/utils v0.0.0-20200729134348-d5654de09c73
	knative.dev/eventing v0.20.1-0.20210128132430-1725902f7e39
	knative.dev/hack v0.0.0-20210120165453-8d623a0af457
	knative.dev/pkg v0.0.0-20210127163530-0d31134d5f4e
)
