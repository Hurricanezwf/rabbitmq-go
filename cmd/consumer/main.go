package main

import (
	"github.com/Hurricanezwf/rabbitmq-go/g"
	"github.com/Hurricanezwf/rabbitmq-go/mq"
	"github.com/Hurricanezwf/toolbox/log"
)

var (
	MQURL = g.MQURL
)

func main() {
	m, err := mq.New(MQURL).Open()
	if err != nil {
		log.Error(err.Error())
		return
	}
	defer m.Close()

	c, err := m.Consumer("test-consume")
	if err != nil {
		log.Error("Create consumer failed, %v", err)
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

	msgC := make(chan []byte, 1)
	defer close(msgC)

	if err = c.SetExchangeBinds(exb).SetMsgCallback(msgC).Open(); err != nil {
		log.Error("Open failed, %v", err)
		return
	}

	//i := 0
	for msg := range msgC {
		//i++
		//if i%5 == 0 {
		//	c.CloseChan()
		//}
		log.Info("Consumer receive msg `%s`", string(msg))
		//time.Sleep(time.Second)
	}
}
