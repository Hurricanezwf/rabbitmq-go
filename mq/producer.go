package mq

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hurricanezwf/toolbox/log"

	"github.com/streadway/amqp"
)

type Producer struct {
	// Producer的名字, "" is OK
	name string

	// MQ实例
	mq *MQ

	// 保护数据并发安全
	mutex sync.RWMutex

	// MQ的会话channel
	ch *amqp.Channel

	// MQ的exchange与其绑定的queues
	exchangeBinds []*ExchangeBinds

	// 生产者confirm开关
	enableConfirm bool
	// 监听publish confirm
	confirmC chan amqp.Confirmation
	// confirm结果检测
	confirm *confirmHelper
	// confirm RRD大小, 理论上RRD越大,冲突概率越小,但响应的资源消耗越大
	// 视最大并发量而定, 默认1024
	rrdSize uint64

	// 监听会话channel关闭
	closeC chan *amqp.Error
	// Producer关闭控制
	stopC chan struct{}

	// Producer 状态
	state uint8
}

func newProducer(name string, mq *MQ) *Producer {
	return &Producer{
		name:    name,
		mq:      mq,
		state:   StateClosed,
		rrdSize: 1024,
	}
}

func (p Producer) Name() string {
	return p.name
}

// CloseChan 仅用于测试使用,勿手动调用
func (p *Producer) CloseChan() {
	p.mutex.Lock()
	p.ch.Close()
	p.mutex.Unlock()
}

// Confirm 是否开启生产者confirm功能, 默认为false, 该选项在Open()前设置.
// 说明: 目前仅实现串行化的confirm, 每次的等待confirm额外需要约50ms,建议上层并发调用Publish
func (p *Producer) Confirm(enable bool) *Producer {
	p.mutex.Lock()
	p.enableConfirm = enable
	p.mutex.Unlock()
	return p
}

// RRDSize 设置无锁环形缓冲区的大小, 默认为1024
// Producer Open之后将无法重置
func (p *Producer) RRDSize(size uint64) *Producer {
	p.mutex.Lock()
	p.rrdSize = size
	p.mutex.Unlock()
	return p
}

func (p *Producer) SetExchangeBinds(eb []*ExchangeBinds) *Producer {
	p.mutex.Lock()
	if p.state != StateOpened {
		p.exchangeBinds = eb
	}
	p.mutex.Unlock()
	return p
}

func (p *Producer) Open() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// 条件检测
	if p.mq == nil {
		return errors.New("MQ: Bad producer")
	}
	if len(p.exchangeBinds) <= 0 {
		return errors.New("MQ: No exchangeBinds found. You should SetExchangeBinds before open.")
	}
	if p.state == StateOpened {
		return errors.New("MQ: Producer had been opened")
	}

	// 创建并初始化channel
	ch, err := p.mq.channel()
	if err != nil {
		return fmt.Errorf("MQ: Create channel failed, %v", err)
	}
	if err = applyExchangeBinds(ch, p.exchangeBinds); err != nil {
		ch.Close()
		return err
	}

	p.ch = ch
	p.state = StateOpened

	// 初始化发送Confirm
	if p.enableConfirm {
		p.confirmC = make(chan amqp.Confirmation, 1) // channel关闭时自动关闭
		p.ch.Confirm(false)
		p.ch.NotifyPublish(p.confirmC)
		if p.confirm == nil {
			p.confirm = newConfirmHelper(p.rrdSize)
		} else {
			p.confirm.Reset()
		}

		go p.listenConfirm()
	}

	// 初始化Keepalive
	if true {
		p.stopC = make(chan struct{})
		p.closeC = make(chan *amqp.Error, 1) // channel关闭时自动关闭
		p.ch.NotifyClose(p.closeC)

		go p.keepalive()
	}

	return nil
}

func (p *Producer) Close() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.stopC != nil {
		close(p.stopC)
		p.stopC = nil
	}
}

// 在同步Publish Confirm模式下, 每次Publish将额外有约50ms的等待时间.如果采用这种模式,建议上层并发publish
func (p *Producer) Publish(exchange, routeKey string, msg *PublishMsg) error {
	if msg == nil {
		return errors.New("MQ: Nil publish msg")
	}

	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if p.state != StateOpened {
		return fmt.Errorf("MQ: Producer unopened, now state is %d", p.state)
	}
	pub := amqp.Publishing{
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		Priority:        msg.Priority,
		Timestamp:       msg.Timestamp,
		Body:            msg.Body,
	}
	if err := p.ch.Publish(exchange, routeKey, false, false, pub); err != nil {
		return fmt.Errorf("MQ: Producer publish failed, %v", err)
	}
	if p.enableConfirm {
		if ack, ok := <-p.confirm.Listen(); !ack || !ok {
			return fmt.Errorf("MQ: Producer publish failed, confirm ack is false. ack:%t, ok:%t", ack, ok)
		}
	}
	return nil
}

func (p *Producer) State() uint8 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.state
}

func (p *Producer) keepalive() {
	select {
	case <-p.stopC:
		// 正常关闭
		log.Warn("MQ: Producer(%s) shutdown normally.", p.name)
		p.mutex.Lock()
		p.ch.Close()
		p.ch = nil
		p.state = StateClosed
		p.mutex.Unlock()

	case err := <-p.closeC:
		if err == nil {
			log.Error("MQ: Producer(%s)'s channel was closed, but Error detail is nil", p.name)
		} else {
			log.Error("MQ: Producer(%s)'s channel was closed, code:%d, reason:%s", p.name, err.Code, err.Reason)
		}

		// channel被异常关闭了
		p.mutex.Lock()
		p.state = StateReopening
		p.mutex.Unlock()

		maxRetry := 99999999
		for i := 0; i < maxRetry; i++ {
			time.Sleep(time.Second)
			if p.mq.State() != StateOpened {
				log.Warn("MQ: Producer(%s) try to recover channel for %d times, but mq's state != StateOpened", p.name, i+1)
				continue
			}
			if e := p.Open(); e != nil {
				log.Error("MQ: Producer(%s) recover channel failed for %d times, Err:%v", p.name, i+1, e)
				continue
			}
			log.Info("MQ: Producer(%s) recover channel OK. Total try %d times", p.name, i+1)
			return
		}
		log.Error("MQ: Producer(%s) try to recover channel over maxRetry(%d), so exit", p.name, maxRetry)
	}
}

func (p *Producer) listenConfirm() {
	for c := range p.confirmC {
		p.confirm.Callback(c.DeliveryTag, c.Ack)
	}
}

func applyExchangeBinds(ch *amqp.Channel, exchangeBinds []*ExchangeBinds) (err error) {
	if ch == nil {
		return errors.New("MQ: Nil producer channel")
	}
	if len(exchangeBinds) <= 0 {
		return errors.New("MQ: Empty exchangeBinds")
	}

	for _, eb := range exchangeBinds {
		if eb.Exch == nil {
			return errors.New("MQ: Nil exchange found.")
		}
		if len(eb.Bindings) <= 0 {
			return fmt.Errorf("MQ: No bindings queue found for exchange(%s)", eb.Exch.Name)
		}
		// declare exchange
		ex := eb.Exch
		if err = ch.ExchangeDeclare(ex.Name, ex.Kind, ex.Durable, ex.AutoDelete, ex.Internal, ex.NoWait, ex.Args); err != nil {
			return fmt.Errorf("MQ: Declare exchange(%s) failed, %v", ex.Name, err)
		}
		// declare and bind queues
		for _, b := range eb.Bindings {
			if b == nil {
				return fmt.Errorf("MQ: Nil binding found, exchange:%s", ex.Name)
			}
			if len(b.Queues) <= 0 {
				return fmt.Errorf("MQ: No queues found for exchange(%s)", ex.Name)
			}
			for _, q := range b.Queues {
				if q == nil {
					return fmt.Errorf("MQ: Nil queue found, exchange:%s", ex.Name)
				}
				if _, err = ch.QueueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, q.Args); err != nil {
					return fmt.Errorf("MQ: Declare queue(%s) failed, %v", q.Name, err)
				}
				if err = ch.QueueBind(q.Name, b.RouteKey, ex.Name, b.NoWait, b.Args); err != nil {
					return fmt.Errorf("MQ: Bind exchange(%s) <--> queue(%s) failed, %v", ex.Name, q.Name, err)
				}
			}
		}
	}
	return nil
}

// confirmHelper 使用RRD来实现了无锁的可复用的回调通知模型
type confirmHelper struct {
	// rrd size
	size uint64
	rrd  []chan bool

	// ackIdx
	lastAckIdx uint64
}

// size should be large enough to avoid read/write concurrently
func newConfirmHelper(size uint64) *confirmHelper {
	if size <= 0 {
		panic("RRD size must be over 0")
	}
	h := &confirmHelper{
		size: size,
		rrd:  make([]chan bool, size),
	}
	return h.Reset()
}

func (h *confirmHelper) Reset() *confirmHelper {
	h.lastAckIdx = uint64(0)
	// 释放并重建RRD
	var ch chan bool
	for i, _ := range h.rrd {
		ch, h.rrd[i] = h.rrd[i], make(chan bool, 1)
		if ch != nil {
			close(ch)
		}
	}
	return h
}

func (h *confirmHelper) Listen() <-chan bool {
	curIdx := atomic.AddUint64(&h.lastAckIdx, uint64(1))
	curIdx = curIdx % h.size
	return h.rrd[curIdx]
}

func (h *confirmHelper) Callback(idx uint64, ack bool) {
	idx = idx % h.size
	h.rrd[idx] <- ack
}
