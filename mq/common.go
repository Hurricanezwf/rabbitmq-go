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
	"time"

	"github.com/streadway/amqp"
)

// Exchange type
var (
	ExchangeDirect  = amqp.ExchangeDirect
	ExchangeFanout  = amqp.ExchangeFanout
	ExchangeTopic   = amqp.ExchangeTopic
	ExchangeHeaders = amqp.ExchangeHeaders
)

// DeliveryMode
var (
	Transient  uint8 = amqp.Transient
	Persistent uint8 = amqp.Persistent
)

// ExchangeBinds exchange ==> routeKey ==> queues
type ExchangeBinds struct {
	Exch     *Exchange
	Bindings []*Binding
}

// Biding routeKey ==> queues
type Binding struct {
	RouteKey string
	Queues   []*Queue
	NoWait   bool       // default is false
	Args     amqp.Table // default is nil
}

// Exchange 基于amqp的Exchange配置
type Exchange struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table // default is nil
}

func DefaultExchange(name string, kind string) *Exchange {
	return &Exchange{
		Name:       name,
		Kind:       kind,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}
}

// Queue 基于amqp的Queue配置
type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

func DefaultQueue(name string) *Queue {
	return &Queue{
		Name:       name,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}
}

// 生产者生产的数据格式
type PublishMsg struct {
	ContentType     string // MIME content type
	ContentEncoding string // MIME content type
	DeliveryMode    uint8  // Transient or Persistent
	Priority        uint8  // 0 to 9
	Timestamp       time.Time
	Body            []byte
}

func NewPublishMsg(body []byte) *PublishMsg {
	return &PublishMsg{
		ContentType:     "application/json",
		ContentEncoding: "",
		DeliveryMode:    Persistent,
		Priority:        uint8(5),
		Timestamp:       time.Now(),
		Body:            body,
	}
}

// 消费者消费选项
type ConsumeOption struct {
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

func DefaultConsumeOption() *ConsumeOption {
	return &ConsumeOption{
		NoWait: true,
	}
}
