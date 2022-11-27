package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"time"
)

var (
	kafkaConsumer *cluster.Consumer
	kafkaBrokers  = []string{"127.0.0.1:9092"}
	kafkaTopic    = "test_topic_1"
	groupId       = "csdn_test_1"
)

func init() {
	var err error
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = -2
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Group.Return.Notifications = true
	kafkaConsumer, err = cluster.NewConsumer(kafkaBrokers, groupId, []string{kafkaTopic}, config)
	if err != nil {
		panic(err.Error())
	}
	if kafkaConsumer == nil {
		panic(fmt.Sprintf("consumer_group is nil. kafka info -> {brokers:%v, topic: %v, group: %v}", kafkaBrokers, kafkaTopic, groupId))
	}
	fmt.Printf("kafka init success, consumer_group -> %v, topic -> %v, ", kafkaConsumer, kafkaTopic)
}

func main() {
	for {
		select {
		case msg, ok := <-kafkaConsumer.Messages():
			if ok {
				fmt.Printf("kafka msg: %s \n", msg.Value)
				kafkaConsumer.MarkOffset(msg, "")
			} else {
				fmt.Printf("kafka 监听服务失败")
			}
		case err, ok := <-kafkaConsumer.Errors():
			if ok {
				fmt.Printf("consumer_group error: %v", err)
			}
		case ntf, ok := <-kafkaConsumer.Notifications():
			if ok {
				fmt.Printf("consumer_group notification: %v", ntf)
			}
		}
	}
}
