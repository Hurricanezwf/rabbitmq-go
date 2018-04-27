package mq

import (
	"fmt"
	"testing"
	"time"
)

const mqUrl = "amqp://zwf:123456@localhost:5672/"

func TestProducer(t *testing.T) {
	m, err := New(mqUrl).Open()
	if err != nil {
		panic(err.Error())
	}

	p, err := m.Producer("test-producer")
	if err != nil {
		panic(fmt.Sprintf("Create producer failed, %v", err))
	}

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

	if err = p.SetExchangeBinds(exb).Confirm(true).Open(); err != nil {
		panic(fmt.Sprintf("Open failed, %v", err))
	}

	for i := 0; i < 1000; i++ {
		if i > 0 && i%3 == 0 {
			p.CloseChan()
		}
		err = p.Publish("exch.unitest", "route.unitest2", NewPublishMsg([]byte(`{"name":"zwf"}`)))
		t.Logf("Produce state:%d, err:%v\n", p.State(), err)
		time.Sleep(time.Second)
	}

	p.Close()
	m.Close()
}
