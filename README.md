# sarama-in-action
sarama in action

## consumer group
```bash
go run consumer_group.go -brokers="127.0.0.1:9092,localhost:9093,localhost:9094" -topics="my-replicated-topic" -group="testGroup2"
```

## transaction producer
```bash
go run producer.go -brokers="127.0.0.1:9092,localhost:9093,localhost:9094" -topic="my-replicated-topic" -producers=1 -records-number=10000
```
