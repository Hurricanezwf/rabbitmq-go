package main

import (
	"log"

	"github.com/Hurricanezwf/rabbitmq-go/g"
	"github.com/Hurricanezwf/rabbitmq-go/mq"
)

var (
	MQURL = g.MQURL
)

func main() {
	m, err := mq.New(MQURL).Open()
	if err != nil {
		log.Printf("[ERROR] %s\n", err.Error())
		return
	}
	defer m.Close()

	c, err := m.Consumer("test-consume")
	if err != nil {
		log.Printf("[ERROR] Create consumer failed, %v\n", err)
		return
	}
	defer c.Close()

	exb := []*mq.ExchangeBinds{
		&mq.ExchangeBinds{
			Exch: mq.DefaultExchange("exch.unitest", mq.ExchangeDirect),
			Bindings: []*mq.Binding{
				&mq.Binding{
					RouteKey: "route.unitest1",
					Queues: []*mq.Queue{
						mq.DefaultQueue("queue.unitest1"),
					},
				},
				&mq.Binding{
					RouteKey: "route.unitest2",
					Queues: []*mq.Queue{
						mq.DefaultQueue("queue.unitest2"),
					},
				},
			},
		},
	}

	msgC := make(chan mq.Delivery, 1)
	defer close(msgC)

	if err = c.SetExchangeBinds(exb).SetMsgCallback(msgC).Open(); err != nil {
		log.Printf("[ERROR] Open failed, %v\n", err)
		return
	}

	for msg := range msgC {
		//log.Info("Tag(%d)", msg.DeliveryTag)
		msg.Ack(true)
		//if i%5 == 0 {
		//	c.CloseChan()
		//}
		//log.Info("Consumer receive msg `%s`", string(msg))
		//time.Sleep(time.Second)
	}
}
