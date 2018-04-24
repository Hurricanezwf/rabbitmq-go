package main

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"net/http"
	_ "net/http/pprof"

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

	for i := 0; i < 1; i++ {
		go func(idx int) {
			p, err := m.Producer(strconv.Itoa(i))
			if err != nil {
				log.Error("Create producer failed, %v", err)
				return
			}
			if err = p.SetExchangeBinds(exb).Open(); err != nil {
				log.Error("Open failed, %v", err)
				return
			}

			ticker := time.NewTicker(1 * time.Millisecond)
			msg := mq.NewPublishMsg([]byte(`{"name":"zwf"}`))
			for {
				select {
				case <-ticker.C:
					err = p.Publish("exch.unitest", "route.unitest2", msg)
					_ = err
					//log.Info("Producer(%d) state:%d, err:%v\n", i, p.State(), err)
				}
			}

		}(i)
	}

	go func() {
		http.ListenAndServe(":11111", nil)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT)
	<-sig
	//for i := 0; i < 1000; i++ {
	//	if i > 0 && i%3 == 0 {
	//		p.CloseChan()
	//	}
	//	err = p.Publish("exch.unitest", "route.unitest2", mq.NewPublishMsg([]byte(`{"name":"zwf"}`)))
	//	log.Info("Produce state:%d, err:%v\n", p.State(), err)
	//	time.Sleep(time.Second)
	//}

	//p.Close()
	//m.Close()
}
