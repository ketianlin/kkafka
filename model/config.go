package model

type Config struct {
	Servers        []string
	ProductConfig  ProductConfig
	ConsumerConfig ConsumerConfig
}

type ProductConfig struct {
	RetryCount             int
	WriteTimeout           int
	PartitionStr           string
	RequiredAcksStr        string
	AllowAutoTopicCreation bool
}

type ConsumerConfig struct {
	CommitInterval int
	GroupID        string
	StartOffsetStr string
}
