package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/ketianlin/kkafka/model"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"github.com/sadlil/gologger"
	"github.com/segmentio/kafka-go"
	"io/ioutil"
	"runtime"
	"strings"
	"time"
)

type consumerClient struct {
	conf       *koanf.Koanf
	confUrl    string
	conn       *kafka.Reader
	closeError error
	config     *model.Config
}

var (
	ConsumerClient = &consumerClient{}
	logger         = gologger.GetLogger()
)

func (r *consumerClient) InitConfig(config *model.Config) {
	r.config = config
}

func (r *consumerClient) Init(kafkaConfigUrl string) {
	if kafkaConfigUrl != "" {
		r.confUrl = kafkaConfigUrl
	}
	if r.confUrl == "" {
		logger.Error("kafka配置Url为空")
		return
	}
}

func (r *consumerClient) newConsumerClientByConfUrl(topic string) *kafka.Reader {
	var confData []byte
	var err error
	if strings.HasPrefix(r.confUrl, "http://") {
		resp, err := grequests.Get(r.confUrl, nil)
		if err != nil {
			logger.Error("Kafka配置下载失败! " + err.Error())
			return nil
		}
		confData = []byte(resp.String())
	} else {
		confData, err = ioutil.ReadFile(r.confUrl)
		if err != nil {
			logger.Error("Kafka本地配置文件{}读取失败:" + r.confUrl + err.Error())
			return nil
		}
	}
	r.conf = koanf.New(".")
	err = r.conf.Load(rawbytes.Provider(confData), yaml.Parser())
	if err != nil {
		logger.Error("Kafka配置解析错误:" + err.Error())
		r.conf = nil
		return nil
	}
	servers := r.conf.Strings("go.data.kafka.servers")
	commitInterval := r.conf.Int("go.data.kafka.consumer.commit_interval")
	groupID := r.conf.String("go.data.kafka.consumer.group_id")
	startOffsetStr := strings.ToLower(r.conf.String("go.data.kafka.consumer.start_offset"))
	startOffset := kafka.FirstOffset
	if startOffsetStr == "last" {
		startOffset = kafka.LastOffset
	}
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        servers, //支持传入多个broker的ip:port
		Topic:          topic,
		CommitInterval: time.Duration(commitInterval) * time.Second, //每隔多长时间自动commit一次offset。即一边读一边向kafka上报读到了哪个位置。
		GroupID:        groupID,                                     //一个Group内消费到的消息不会重复
		StartOffset:    startOffset,                                 //当一个特定的partition没有commited offset时(比如第一次读一个partition，之前没有commit过)，通过StartOffset指定从第一个还是最后一个位置开始消费。StartOffset的取值要么是FirstOffset要么是LastOffset，LastOffset表示Consumer启动之前生成的老数据不管了。仅当指定了GroupID时，StartOffset才生效。
	})
}

func (r *consumerClient) newConsumerClientByConfig(topic string) *kafka.Reader {
	startOffset := kafka.FirstOffset
	if r.config.ConsumerConfig.StartOffsetStr == "last" {
		startOffset = kafka.LastOffset
	}
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        r.config.Servers, //支持传入多个broker的ip:port
		Topic:          topic,
		CommitInterval: time.Duration(r.config.ConsumerConfig.CommitInterval) * time.Second, //每隔多长时间自动commit一次offset。即一边读一边向kafka上报读到了哪个位置。
		GroupID:        r.config.ConsumerConfig.GroupID,                                     //一个Group内消费到的消息不会重复
		StartOffset:    startOffset,                                                         //当一个特定的partition没有commited offset时(比如第一次读一个partition，之前没有commit过)，通过StartOffset指定从第一个还是最后一个位置开始消费。StartOffset的取值要么是FirstOffset要么是LastOffset，LastOffset表示Consumer启动之前生成的老数据不管了。仅当指定了GroupID时，StartOffset才生效。
	})
}

func (r *consumerClient) GetConnection(topic string) *kafka.Reader {
	if r.conn == nil {
		if r.confUrl != "" {
			r.conn = r.newConsumerClientByConfUrl(topic)
		} else if r.config != nil {
			r.conn = r.newConsumerClientByConfig(topic)
		} else {
			logger.Error("获取kafka连接失败,kafka没有初始化! ")
		}
	}
	return r.conn
}

func (r *consumerClient) Close() {
	if r.conn != nil {
		defer func() {
			if e := recover(); e != nil {
				switch e := e.(type) {
				case string:
					r.closeError = errors.New(e)
				case runtime.Error:
					r.closeError = errors.New(e.Error())
				case error:
					r.closeError = e
				default:
					r.closeError = errors.New("Kafka关闭消费者client错误")
				}
			}
		}()
		err := r.conn.Close()
		if err != nil {
			logger.Error(fmt.Sprintf("Kafka关闭消费者client错误:%s\n", err.Error()))
			r.closeError = err
			return
		}
	}
	r.conn = nil
}

func (r *consumerClient) MessageListener(topic string, listener func(msg kafka.Message), callbacks ...func(err error)) {
	reader := r.GetConnection(topic)
	for { //消息队列里随时可能有新消息进来，所以这里是死循环，类似于读Channel
		if message, err := reader.ReadMessage(context.Background()); err != nil {
			logger.Error(fmt.Sprintf("从 Kafka 读取消息失败: %v", err))
			if len(callbacks) > 0 {
				callbacks[0](err)
			}
			break
		} else {
			//offset := message.Offset
			//fmt.Printf("time=%v, topic=%s, partition=%d, offset=%d, key=%s, message content=%s\n", message.Time, message.Topic, message.Partition, offset, string(message.Key), string(message.Value))
			listener(message)
		}
	}
}
