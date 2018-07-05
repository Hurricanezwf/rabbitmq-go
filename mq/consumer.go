package mq

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Delivery struct {
	amqp.Delivery
}

type Consumer struct {
	// Consumer的名字, "" is OK
	name string

	// MQ实例
	mq *MQ

	// 保护数据并发安全
	mutex sync.RWMutex

	// MQ的会话channel
	ch *amqp.Channel

	// MQ的exchange与其绑定的queues
	exchangeBinds []*ExchangeBinds

	// Oos prefetch
	prefetch int

	// 上层用于接收消费出来的消息的管道
	callback chan<- Delivery

	// 监听会话channel关闭
	closeC chan *amqp.Error
	// Consumer关闭控制
	stopC chan struct{}

	// Consumer状态
	state uint8
}

func newConsumer(name string, mq *MQ) *Consumer {
	return &Consumer{
		name:  name,
		mq:    mq,
		stopC: make(chan struct{}),
	}
}

func (c Consumer) Name() string {
	return c.name
}

// CloseChan 该接口仅用于测试使用, 勿手动调用
func (c *Consumer) CloseChan() {
	c.mutex.Lock()
	c.ch.Close()
	c.mutex.Unlock()
}

func (c *Consumer) SetExchangeBinds(eb []*ExchangeBinds) *Consumer {
	c.mutex.Lock()
	if c.state != StateOpened {
		c.exchangeBinds = eb
	}
	c.mutex.Unlock()
	return c
}

func (c *Consumer) SetMsgCallback(cb chan<- Delivery) *Consumer {
	c.mutex.Lock()
	c.callback = cb
	c.mutex.Unlock()
	return c
}

// SetQos 设置channel粒度的Qos, prefetch取值范围[0,∞), 默认为0
// 如果想要RoundRobin地进行消费，设置prefetch为1即可
// 注意:在调用Open前设置
func (c *Consumer) SetQos(prefetch int) *Consumer {
	c.mutex.Lock()
	c.prefetch = prefetch
	c.mutex.Unlock()
	return c
}

func (c *Consumer) Open() error {
	// Open期间不允许对channel做任何操作
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 参数校验
	if c.mq == nil {
		return errors.New("MQ: Bad consumer")
	}
	if len(c.exchangeBinds) <= 0 {
		return errors.New("MQ: No exchangeBinds found. You should SetExchangeBinds brefore open.")
	}

	// 状态检测
	if c.state == StateOpened {
		return errors.New("MQ: Consumer had been opened")
	}

	// 初始化channel
	ch, err := c.mq.channel()
	if err != nil {
		return fmt.Errorf("MQ: Create channel failed, %v", err)
	}

	err = func(ch *amqp.Channel) error {
		var e error
		if e = applyExchangeBinds(ch, c.exchangeBinds); e != nil {
			return e
		}
		if e = ch.Qos(c.prefetch, 0, false); e != nil {
			return e
		}
		return nil
	}(ch)
	if err != nil {
		ch.Close()
		return fmt.Errorf("MQ: %v", err)
	}

	c.ch = ch
	c.state = StateOpened
	c.stopC = make(chan struct{})
	c.closeC = make(chan *amqp.Error, 1)
	c.ch.NotifyClose(c.closeC)

	// 开始循环消费
	opt := DefaultConsumeOption()
	notify := make(chan error, 1)
	c.consume(opt, notify)
	for e := range notify {
		if e != nil {
			log.Printf("[ERROR] %v\n", e)
			continue
		}
		break
	}
	close(notify)

	// 健康检测
	go c.keepalive()

	return nil
}

func (c *Consumer) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.stopC != nil {
		close(c.stopC)
		c.stopC = nil
	}
}

// notifyErr 向上层抛出错误, 如果error为空表示执行完成.由上层负责关闭channel
func (c *Consumer) consume(opt *ConsumeOption, notifyErr chan<- error) {
	for idx, eb := range c.exchangeBinds {
		if eb == nil {
			notifyErr <- fmt.Errorf("MQ: ExchangeBinds[%d] is nil, consumer(%s)", idx, c.name)
			continue
		}
		for i, b := range eb.Bindings {
			if b == nil {
				notifyErr <- fmt.Errorf("MQ: Binding[%d] is nil, ExchangeBinds[%d], consumer(%s)", i, idx, c.name)
				continue
			}
			for qi, q := range b.Queues {
				if q == nil {
					notifyErr <- fmt.Errorf("MQ: Queue[%d] is nil, ExchangeBinds[%d], Biding[%d], consumer(%s)", qi, idx, i, c.name)
					continue
				}
				delivery, err := c.ch.Consume(q.Name, "", opt.AutoAck, opt.Exclusive, opt.NoLocal, opt.NoWait, opt.Args)
				if err != nil {
					notifyErr <- fmt.Errorf("MQ: Consumer(%s) consume queue(%s) failed, %v", c.name, q.Name, err)
					continue
				}
				go c.deliver(delivery)
			}
		}
	}
	notifyErr <- nil
}

func (c *Consumer) deliver(delivery <-chan amqp.Delivery) {
	for d := range delivery {
		if c.callback != nil {
			c.callback <- Delivery{d}
		}
	}
}

func (c *Consumer) State() uint8 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.state
}

func (c *Consumer) keepalive() {
	select {
	case <-c.stopC:
		// 正常关闭
		log.Printf("[WARN] Consumer(%s) shutdown normally\n", c.Name())
		c.mutex.Lock()
		c.ch.Close()
		c.ch = nil
		c.state = StateClosed
		c.mutex.Unlock()

	case err := <-c.closeC:
		if err == nil {
			log.Printf("[ERROR] MQ: Consumer(%s)'s channel was closed, but Error detail is nil\n", c.name)
		} else {
			log.Printf("[ERROR] MQ: Consumer(%s)'s channel was closed, code:%d, reason:%s\n", c.name, err.Code, err.Reason)
		}

		// channel被异常关闭了
		c.mutex.Lock()
		c.state = StateReopening
		c.mutex.Unlock()

		maxRetry := 99999999
		for i := 0; i < maxRetry; i++ {
			time.Sleep(time.Second)
			if c.mq.State() != StateOpened {
				log.Printf("[WARN] MQ: Consumer(%s) try to recover channel for %d times, but mq's state != StateOpened\n", c.name, i+1)
				continue
			}
			if e := c.Open(); e != nil {
				log.Printf("[WARN] MQ: Consumer(%s) recover channel failed for %d times, Err:%v\n", c.name, i+1, e)
				continue
			}
			log.Printf("[INFO] MQ: Consumer(%s) recover channel OK. Total try %d times\n", c.name, i+1)
			return
		}
		log.Printf("[ERROR] MQ: Consumer(%s) try to recover channel over maxRetry(%d), so exit\n", c.name, maxRetry)
	}
}
