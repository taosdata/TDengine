package connector

type MQTTConnector interface {
	SubscribeMultiple(topics map[string]int) error
	Publish(topic string, qos byte, retained bool, payload []byte) error
	Stop()
}

type OnMessage func(qos byte, topic string, payload []byte)
type OnConnect func()
type OnDisconnected func(err error)
