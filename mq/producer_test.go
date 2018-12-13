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
