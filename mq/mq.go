// mq封装了RabbitMQ的生产者消费者
// 建议在大流量的情况下生产者和消费者的TCP连接分离,以免影响传输效率, 一个MQ对象对应一个TCP连接

package mq

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

var (
	StateClosed    = uint8(0)
	StateOpened    = uint8(1)
	StateReopening = uint8(2)
)

type MQ struct {
	// RabbitMQ连接的url
	url string

	// 保护内部数据并发读写
	mutex sync.RWMutex

	// RabbitMQ TCP连接
	conn *amqp.Connection

	producers []*Producer
	consumers []*Consumer

	// RabbitMQ 监听连接错误
	closeC chan *amqp.Error
	// 监听用户手动关闭
	stopC chan struct{}

	// MQ状态
	state uint8
}

func New(url string) *MQ {
	return &MQ{
		url:       url,
		producers: make([]*Producer, 0, 1),
		state:     StateClosed,
	}
}

func (m *MQ) Open() (mq *MQ, err error) {
	// 进行Open期间不允许做任何跟连接有关的事情
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.state == StateOpened {
		return m, errors.New("MQ: Had been opened")
	}

	if m.conn, err = m.dial(); err != nil {
		return m, fmt.Errorf("MQ: Dial err: %v", err)
	}

	m.state = StateOpened
	m.stopC = make(chan struct{})
	m.closeC = make(chan *amqp.Error, 1)
	m.conn.NotifyClose(m.closeC)

	go m.keepalive()

	return m, nil
}

func (m *MQ) Close() {
	m.mutex.Lock()

	// close producers
	for _, p := range m.producers {
		p.Close()
	}
	m.producers = m.producers[:0]

	// close consumers
	for _, c := range m.consumers {
		c.Close()
	}
	m.consumers = m.consumers[:0]

	// close mq connection
	if m.stopC != nil {
		close(m.stopC)
	}

	m.mutex.Unlock()

	// wait done
	for m.State() != StateClosed {
		time.Sleep(time.Second)
	}
}

func (m *MQ) Producer(name string) (*Producer, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.state != StateOpened {
		return nil, fmt.Errorf("MQ: Not initialized, now state is %d", m.State)
	}
	p := newProducer(name, m)
	m.producers = append(m.producers, p)
	return p, nil
}

func (m *MQ) Consumer(name string) (*Consumer, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.state != StateOpened {
		return nil, fmt.Errorf("MQ: Not initialized, now state is %d", m.State)
	}
	c := newConsumer(name, m)
	m.consumers = append(m.consumers, c)
	return c, nil
}

func (m *MQ) State() uint8 {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.state
}

func (m *MQ) keepalive() {
	select {
	case <-m.stopC:
		// 正常关闭
		log.Println("[WARN] MQ: Shutdown normally.")
		m.mutex.Lock()
		m.conn.Close()
		m.state = StateClosed
		m.mutex.Unlock()

	case err := <-m.closeC:
		if err == nil {
			log.Println("[ERROR] MQ: Disconnected with MQ, but Error detail is nil")
		} else {
			log.Printf("[ERROR] MQ: Disconnected with MQ, code:%d, reason:%s\n", err.Code, err.Reason)
		}

		// tcp连接中断, 重新连接
		m.mutex.Lock()
		m.state = StateReopening
		m.mutex.Unlock()

		maxRetry := 99999999
		for i := 0; i < maxRetry; i++ {
			time.Sleep(time.Second)
			if _, e := m.Open(); e != nil {
				log.Printf("[ERROR] MQ: Connection recover failed for %d times, %v\n", i+1, e)
				continue
			}
			log.Printf("[INFO] MQ: Connection recover OK. Total try %d times\n", i+1)
			return
		}
		log.Printf("[ERROR] MQ: Try to reconnect to MQ failed over maxRetry(%d), so exit.\n", maxRetry)
	}
}

//func (m *MQ) Consumer() *Consumer {
//	return nil
//}

func (m *MQ) channel() (*amqp.Channel, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.conn.Channel()
}

func (m MQ) dial() (*amqp.Connection, error) {
	return amqp.Dial(m.url)
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
