package main

import (
	"github.com/gmbyapa/kafka-connector"
	"os"
	"os/signal"
)

func main() {

	// initiate worker
	worker, err := kafka_connect.NewConnectWorker()
	if err != nil {
		kafka_connect.Logger.Fatal(`connect.worker`, err)
	}

	if err := worker.Start(); err != nil {
		kafka_connect.Logger.Fatal(`connect.worker`, err)
	}

	http := worker.Http()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	go func() {
		<-sig
		if err := worker.Stop(); err != nil {
			kafka_connect.Logger.Fatal(`connect.worker`, err)
		}

		if err := http.Stop(); err != nil {
			kafka_connect.Logger.Fatal(`connect.worker`, err)
		}

	}()

	http.Start()
}
