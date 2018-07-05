package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/Hurricanezwf/rabbitmq-go/g"
	"github.com/Hurricanezwf/rabbitmq-go/mq"
)

var (
	MQURL     = g.MQURL
	PProfAddr = ":11111"
)

func main() {
	m, err := mq.New(MQURL).Open()
	if err != nil {
		log.Printf("[ERROR] %s\n", err.Error())
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

	// 使用不同的producer并发publish
	for i := 0; i < 1; i++ {
		go func(idx int) {
			p, err := m.Producer(strconv.Itoa(i))
			if err != nil {
				log.Printf("[ERROR] Create producer failed, %v\n", err)
				return
			}
			if err = p.SetExchangeBinds(exb).Confirm(true).Open(); err != nil {
				log.Printf("[ERROR] Open failed, %v\n", err)
				return
			}

			// 使用同一个producer并发publish
			for j := 0; j < 1; j++ {
				go func(v int) {
					for {
						v++
						msg := mq.NewPublishMsg([]byte(fmt.Sprintf(`{"name":"zwf-%d"}`, v)))
						err = p.Publish("exch.unitest", "route.unitest2", msg)
						if err != nil {
							log.Printf("[ERROR] %s\n", err.Error())
						}
						//log.Info("Producer(%d) state:%d, err:%v\n", i, p.State(), err)
					}
				}(j)
				time.Sleep(1 * time.Second)
			}

		}(i)
	}

	go func() {
		http.ListenAndServe(PProfAddr, nil)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
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
