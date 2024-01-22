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
	"sync"
	"testing"
	"time"
)

var (
	logger    gologger.GoLogger
	topicName = "dm_topic"
)

type Member struct {
	Id           int       `json:"id"`
	Name         string    `json:"name"`
	Email        string    `json:"email"`
	Status       bool      `json:"status"`
	RegisterTime time.Time `json:"register_time"`
}

func init() {
	logger = gologger.GetLogger(gologger.FILE, "logs/aaa.log")
	logger.Info("开始测试了")
}

func TestInit(t *testing.T) {
	//logger = gologger.GetLogger(gologger.FILE, "logs/aaa.log")
	logger.Info(fmt.Sprintf("你唉唉唉:%s", t.Name()))
}

func TestRocketMqByConfig(t *testing.T) {
	mc := model.Config{
		Servers: []string{"192.168.20.10:9092"},
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
	//offset := message.Offset
	//fmt.Printf("接收到原始消息: time=%v, topic=%s, partition=%d, offset=%d, key=%s, message content=%s\n", message.Time, message.Topic, message.Partition, offset, string(message.Key), string(message.Value))
	safeHandler(message, func(message kafka.Message) {
		//var m Member
		//_ = json.Unmarshal(message.Value, &m)
		logger.Info(fmt.Sprintf("【%d】收到:%s", message.Offset, string(message.Value)))
		//fmt.Println("打印序列化后消息：", m)
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
	wg := &sync.WaitGroup{}
	ww := &sync.Mutex{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := Member{
				Name:         fmt.Sprintf("吊毛666666-%d", i),
				Email:        fmt.Sprintf("diaomao-%d@qq.com", i),
				Status:       true,
				Id:           666666 + i,
				RegisterTime: getCurTime(),
			}
			mJson, _ := json.Marshal(m)
			key := fmt.Sprintf("dm-1%d", i)
			ww.Lock()
			err := producer.ProducerClient.SendMessage(kafka.Message{
				Topic: topicName,
				Value: mJson,
				Key:   []byte(key),
			})
			ww.Unlock()
			if err != nil {
				logger.Error(fmt.Sprintf("%d: kafka发送消息失败", i))
				return
			}
		}(i)
		//time.Sleep(1 * time.Second)
	}
	wg.Wait()
	logger.Info("发送成功")
}
