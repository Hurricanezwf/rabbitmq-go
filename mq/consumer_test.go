package mq

import (
	"fmt"
	"testing"
	"time"
)

func TestConsumer(t *testing.T) {
	m, err := New(mqUrl).Open()
	if err != nil {
		panic(err.Error())
	}
	defer m.Close()

	c, err := m.Consumer("test-consume")
	if err != nil {
		panic(fmt.Sprintf("Create consumer failed, %v", err))
	}
	defer c.Close()

	exb := []*ExchangeBinds{
		&ExchangeBinds{
			Exch: DefaultExchange("exch.unitest", ExchangeDirect),
			Bindings: []*Binding{
				&Binding{
					RouteKey: "route.unitest1",
					Queues: []*Queue{
						DefaultQueue("queue.unitest1"),
					},
				},
				&Binding{
					RouteKey: "route.unitest2",
					Queues: []*Queue{
						DefaultQueue("queue.unitest2"),
					},
				},
			},
		},
	}

	msgC := make(chan *Delivery, 1)
	defer close(msgC)

	if err = c.SetExchangeBinds(exb).SetMsgCallback(msgC).Open(); err != nil {
		panic(fmt.Sprintf("Open failed, %v", err))
	}

	i := 0
	for msg := range msgC {
		i++
		if i%5 == 0 {
			c.CloseChan()
		}
		t.Logf("Consumer receive msg `%s`\n", string(msg.Body))
		time.Sleep(time.Second)
	}
}
