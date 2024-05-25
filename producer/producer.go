package producer

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"ccrayz/exmaple-kafka/producer/topics"
)

/*
debug]
	generic – 일반적인 클라이언트와 라이브러리 관련 정보.
	broker – 브로커와 관련된 정보, 예를 들어 연결, 연결 해제, 리더 선출 등.
	topic – 토픽 메타데이터와 관련된 정보.
	metadata – 브로커에서 메타데이터를 가져올 때의 정보.
	feature – 클라이언트와 브로커 간의 기능 협상 정보.
	queue – 내부 메시지 큐에 대한 정보.
	msg – 메시지 송수신 관련 로깅.
	protocol – 프로토콜 처리와 관련된 정보.
	cgrp – 컨슈머 그룹 관리와 관련된 정보.
	security – 보안 설정과 관련된 정보, 예를 들어 인증, 인가 등.
	fetch – 데이터 가져오기(fetching)에 관련된 정보.
	interceptor – 인터셉터 작동 관련 정보.
	plugin – 플러그인 관련 정보.
	consumer – 컨슈머 관련 디버그 정보.
	admin – 관리자 API 관련 정보.
*/

func Start(bootstrapServers string, topic string) {
	config := kafka.ConfigMap{
		"bootstrap.servers":                     bootstrapServers,
		"enable.idempotence":                    true,
		"debug":                                 "generic", //"generic,broker,topic,metadata,queue,msg",
		"client.id":                             "my-producer",
		"acks":                                  -1, // all
		"retries":                               2,
		"partitioner":                           "consistent_random",
		"delivery.timeout.ms":                   120000,
		"max.in.flight.requests.per.connection": 5, // producer와 broker 사이 최대 5개의 메제지 배치가 전송될 수 있음 (병렬)

		"linger.ms":        20,        // 배치를 기다리는 시간
		"batch.size":       32 * 1024, // 배치보다 큰값이오면 바로 전송됨
		"compression.type": "snappy",
	}
	fmt.Println(config)

	p, err := kafka.NewProducer(&config)
	if err != nil {
		panic(err)
	}

	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	if topic == "wikimedia" {
		topics.WikiMedia(p)
	}
	if topic == "chaindata_blocknumber" {
		topics.ChaindataBlocknumber(p)
	}
}
