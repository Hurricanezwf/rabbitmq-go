### 简介
基于golang的[amqp](https://godoc.org/github.com/streadway/amqp)库封装的可快速使用的rabbitmq的生产者和消费者.
* 支持自动断线重连
* 简化了使用amqp库时的各种设置, 详细使用方法参照cmd/producer & cmd/consumer的demo实现
* **最好先看下封装的源码判断是否符合自己的场景要求,而后决定是否使用**
* 如果决定使用, 你可能需要修改一下mq包中的log库的指向.


### 编译
* 推荐Go1.9+
* 直接make

### 如何嵌入到自己的代码中?
* import "github.com/Hurricanezwf/rabbitmq-go/mq"
* 参照cmd中的生产者和消费者demo使用
