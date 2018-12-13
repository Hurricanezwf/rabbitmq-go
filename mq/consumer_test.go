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
