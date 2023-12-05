package producer

import (
	"context"
	"fmt"
	"github.com/ketianlin/kkafka/model"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/levigross/grequests"
	"github.com/sadlil/gologger"
	"github.com/segmentio/kafka-go"
	"io/ioutil"
	"strings"
	"time"
)

type producerClient struct {
	conf       *koanf.Koanf
	confUrl    string
	closeError error
	conn       *kafka.Writer
	retryCount int
}

var (
	ProducerClient = &producerClient{}
	logger         = gologger.GetLogger()
)

func (r *producerClient) InitConfig(config *model.Config) {
	var balancer kafka.Balancer
	if config.ProductConfig.PartitionStr == "hash" {
		balancer = &kafka.Hash{}
	} else {
		balancer = &kafka.RoundRobin{}
	}
	var requiredAcks kafka.RequiredAcks
	if config.ProductConfig.RequiredAcksStr == "none" {
		requiredAcks = kafka.RequireNone
	} else if config.ProductConfig.RequiredAcksStr == "one" {
		requiredAcks = kafka.RequireOne
	} else {
		requiredAcks = kafka.RequireAll
	}
	r.retryCount = config.ProductConfig.RetryCount // 写入消息失败是，允许重试几次
	r.conn = &kafka.Writer{
		Addr:                   kafka.TCP(config.Servers...),                                   // 不定长参数，支持传入多个broker的ip:port
		Balancer:               balancer,                                                       // 把message的key进行hash，确定partition
		WriteTimeout:           time.Duration(config.ProductConfig.WriteTimeout) * time.Second, // 设定写超时
		RequiredAcks:           requiredAcks,                                                   // RequireNone不需要等待ack返回，效率最高，安全性最低；RequireOne只需要确保Leader写入成功就可以发送下一条消息；RequiredAcks需要确保Leader和所有Follower都写入成功才可以发送下一条消息。
		AllowAutoTopicCreation: config.ProductConfig.AllowAutoTopicCreation,                    // Topic不存在时自动创建。生产环境中一般设为false，由运维管理员创建Topic并配置partition数目
	}
}

func (r *producerClient) Init(kafkaConfigUrl string) {
	if kafkaConfigUrl != "" {
		r.confUrl = kafkaConfigUrl
	}
	if r.confUrl == "" {
		logger.Error("kafka配置Url为空")
		return
	}
	if r.conf == nil {
		var confData []byte
		var err error
		if strings.HasPrefix(r.confUrl, "http://") {
			resp, err := grequests.Get(r.confUrl, nil)
			if err != nil {
				logger.Error("kafka配置下载失败!" + err.Error())
				return
			}
			confData = []byte(resp.String())
		} else {
			confData, err = ioutil.ReadFile(r.confUrl)
			if err != nil {
				logger.Error("kafka本地配置文件" + r.confUrl + ", 读取失败:" + err.Error())
				return
			}
		}
		r.conf = koanf.New(".")
		err = r.conf.Load(rawbytes.Provider(confData), yaml.Parser())
		if err != nil {
			logger.Error("kafka配置解析错误:" + err.Error())
			r.conf = nil
			return
		}
		servers := r.conf.Strings("go.data.kafka.servers")
		writeTimeout := r.conf.Int("go.data.kafka.producer.write_timeout")
		partitionStr := strings.ToLower(r.conf.String("go.data.kafka.producer.partition"))
		var balancer kafka.Balancer
		if partitionStr == "hash" {
			balancer = &kafka.Hash{}
		} else {
			balancer = &kafka.RoundRobin{}
		}
		requiredAcksStr := strings.ToLower(r.conf.String("go.data.kafka.producer.required_acks"))
		var requiredAcks kafka.RequiredAcks
		if requiredAcksStr == "none" {
			requiredAcks = kafka.RequireNone
		} else if requiredAcksStr == "one" {
			requiredAcks = kafka.RequireOne
		} else {
			requiredAcks = kafka.RequireAll
		}
		allowAutoTopicCreation := r.conf.Bool("go.data.kafka.producer.allow_auto_topic_creation")
		r.retryCount = r.conf.Int("go.data.kafka.producer.retry_count") // 写入消息失败是，允许重试几次
		r.conn = &kafka.Writer{
			Addr:                   kafka.TCP(servers...),                     // 不定长参数，支持传入多个broker的ip:port
			Balancer:               balancer,                                  // 把message的key进行hash，确定partition
			WriteTimeout:           time.Duration(writeTimeout) * time.Second, // 设定写超时
			RequiredAcks:           requiredAcks,                              // RequireNone不需要等待ack返回，效率最高，安全性最低；RequireOne只需要确保Leader写入成功就可以发送下一条消息；RequiredAcks需要确保Leader和所有Follower都写入成功才可以发送下一条消息。
			AllowAutoTopicCreation: allowAutoTopicCreation,                    // Topic不存在时自动创建。生产环境中一般设为false，由运维管理员创建Topic并配置partition数目
			//Topic:                  topic,                 //为所有message指定统一的topic。如果这里不指定统一的Topic，则创建kafka.Message{}时需要分别指定Topic
		}
	}
}

func (r *producerClient) GetConnection() *kafka.Writer {
	return r.conn
}

func (r *producerClient) Close() {
	if r.conn != nil {
		err := r.conn.Close()
		if err != nil {
			logger.Error(fmt.Sprintf("kafka关闭生产者client错误:%s\n", err.Error()))
			r.closeError = err
			return
		}
	}
	r.conn = nil
}

func (r *producerClient) SendMessage(msg kafka.Message, retryCounts ...int) error {
	retryCount := r.retryCount
	if len(retryCounts) > 0 {
		retryCount = retryCounts[0]
	}
	ctx := context.Background()
	for i := 0; i < retryCount; i++ {
		if err := r.conn.WriteMessages(ctx, msg); err != nil {
			// 首次写一个新的Topic时，会发生LeaderNotAvailable错误，重试一次就好了
			if err == kafka.LeaderNotAvailable {
				time.Sleep(500 * time.Millisecond)
				continue
			} else {
				logger.Error(fmt.Sprintf("kafka发送消息错误:%s\n", err.Error()))
				return err
			}
		} else {
			break //只要成功一次就不再尝试下一次了
		}
	}
	return nil
}
