// Copyright 2018 Hurricanezwf. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"log"
	"time"

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

	c.SetExchangeBinds(exb)
	c.SetMsgCallback(msgC)
	c.SetQos(10)
	if err = c.Open(); err != nil {
		log.Printf("[ERROR] Open failed, %v\n", err)
		return
	}

	for msg := range msgC {
		log.Printf("Tag(%d) Body: %s\n", msg.DeliveryTag, string(msg.Body))
		msg.Ack(true)
		//if i%5 == 0 {
		//	c.CloseChan()
		//}
		//log.Info("Consumer receive msg `%s`", string(msg))
		time.Sleep(time.Second)
	}
}
