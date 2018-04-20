all: producer consumer

producer:
	go fmt ./cmd/producer
	go build ./cmd/producer

consumer:
	go fmt ./cmd/consumer
	go build ./cmd/consumer

clean:
	rm -rf ./producer
	rm -rf ./consumer
