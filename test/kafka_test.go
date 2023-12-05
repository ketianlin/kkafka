package test

import (
	"encoding/json"
	"fmt"
	"github.com/ketianlin/kkafka/consumer"
	"github.com/ketianlin/kkafka/model"
	"github.com/ketianlin/kkafka/producer"
	"github.com/sadlil/gologger"
	"github.com/segmentio/kafka-go"
	"runtime/debug"
	"testing"
	"time"
)

var (
	logger    = gologger.GetLogger()
	topicName = "dm_topic"
)

type Member struct {
	Id           int       `json:"id"`
	Name         string    `json:"name"`
	Email        string    `json:"email"`
	Status       bool      `json:"status"`
	RegisterTime time.Time `json:"register_time"`
}

func TestRocketMqByConfig(t *testing.T) {
	mc := model.Config{
		Servers: []string{"192.168.20.130:9092"},
		ProductConfig: model.ProductConfig{
			RetryCount:             2,
			WriteTimeout:           1,
			PartitionStr:           "hash",
			RequiredAcksStr:        "none",
			AllowAutoTopicCreation: true,
		},
		ConsumerConfig: model.ConsumerConfig{
			CommitInterval: 1,
			GroupID:        "recommend_biz",
			StartOffsetStr: "first",
		},
	}
	producer.ProducerClient.InitConfig(&mc)
	defer producer.ProducerClient.Close()
	consumer.ConsumerClient.InitConfig(&mc)
	defer consumer.ConsumerClient.Close()
	go ListenRMQ()
	SendMessage()
	select {}
}

type testEventHandler struct{}

var Test testEventHandler

func (o *testEventHandler) handleExpireMsg(message kafka.Message) {
	offset := message.Offset
	fmt.Printf("接收到原始消息: time=%v, topic=%s, partition=%d, offset=%d, key=%s, message content=%s\n", message.Time, message.Topic, message.Partition, offset, string(message.Key), string(message.Value))
	safeHandler(message, func(message kafka.Message) {
		var m Member
		_ = json.Unmarshal(message.Value, &m)
		fmt.Println("打印序列化后消息：", m)
	})
}

func safeHandler(msg kafka.Message, handler func(msg kafka.Message)) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				fmt.Printf("goroutine process panic %v stack: %v", err, string(debug.Stack()))
			}
		}()
		handler(msg)
	}()
}

func ListenRMQ() {
	logger.Debug("kafka客户端 获取连接成功")
	consumer.ConsumerClient.MessageListener(topicName, Test.handleExpireMsg, func(err error) {
		fmt.Println("MessageListener error: ", err.Error())
	})
}

func getCurTime() time.Time {
	var cstSh, _ = time.LoadLocation("Asia/Shanghai") //上海
	return time.Now().In(cstSh)
}

func SendMessage() {
	logger.Debug("kafka生产者发消息拉……")
	for i := 0; i < 10; i++ {
		m := Member{
			Name:         fmt.Sprintf("吊毛666-%d", i),
			Email:        fmt.Sprintf("diaomao-%d@qq.com", i),
			Status:       true,
			Id:           666 + i,
			RegisterTime: getCurTime(),
		}
		mJson, _ := json.Marshal(m)
		key := fmt.Sprintf("dm-1%d", i)
		err := producer.ProducerClient.SendMessage(kafka.Message{
			Topic: topicName,
			Value: mJson,
			Key:   []byte(key),
		})
		if err != nil {
			logger.Error(fmt.Sprintf("%d: kafka发送消息失败", i))
			return
		}
		time.Sleep(1 * time.Second)
	}
	logger.Info("发送成功")
}
