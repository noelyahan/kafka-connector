package main

import (
	"github.com/Shopify/sarama"
	"testing"
)

var brokers = []string{"127.0.0.1:9092"}

func newProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)

	return producer, err
}

func prepareMessage(topic, message string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(message),
	}

	return msg
}

func TestBenchmarkLoad(b *testing.T) {
	p, err := newProducer()
	if err != nil {
		b.Error(err)
	}
	msg := prepareMessage(`fire-store-source-topic1`, `{"avatar_url":"https://github.com/images/error/octocat_happy.gif","bio":"There once was...","blog":"https://github.com/blog","company":"GitHub","created_at":"2008-01-14T04:33:35Z","email":"octocat@github.com","events_url":"https://api.github.com/users/octocat/events{/privacy}","followers":20,"followers_url":"https://api.github.com/users/octocat/followers","following":0,"following_url":"https://api.github.com/users/octocat/following{/other_user}","gists_url":"https://api.github.com/users/octocat/gists{/gist_id}","gravatar_id":"","hireable":false,"html_url":"https://github.com/octocat","id":1,"location":"San Francisco","login":"octocat","name":"noel yahan","node_id":"MDQ6VXNlcjE=","organizations_url":"https://api.github.com/users/octocat/orgs","public_gists":1,"public_repos":2,"received_events_url":"https://api.github.com/users/octocat/received_events","repos_url":"https://api.github.com/users/octocat/repos","site_admin":false,"starred_url":"https://api.github.com/users/octocat/starred{/owner}{/repo}","subscriptions_url":"https://api.github.com/users/octocat/subscriptions","type":"User","updated_at":"2008-01-14T04:33:35Z","url":"https://api.github.com/users/octocat"}`)
	for i := 0; i < 1000; i++ {
		_, _, err = p.SendMessage(msg)
		//time.Sleep(1 * time.Second)
		if err != nil {
			b.Error(err)
		}
	}
}
