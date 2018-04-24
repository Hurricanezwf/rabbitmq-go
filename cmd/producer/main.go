package main

import (
	"time"

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

	p, err := m.Producer("test-producer")
	if err != nil {
		log.Error("Create producer failed, %v", err)
		return
	}

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

	if err = p.SetExchangeBinds(exb).Open(); err != nil {
		log.Error("Open failed, %v", err)
		return
	}

	for i := 0; i < 1000; i++ {
		if i > 0 && i%3 == 0 {
			p.CloseChan()
		}
		err = p.Publish("exch.unitest", "route.unitest2", mq.NewPublishMsg([]byte(`{"name":"zwf"}`)))
		log.Info("Produce state:%d, err:%v\n", p.State(), err)
		time.Sleep(time.Second)
	}

	p.Close()
	m.Close()
}
