.PHONY: build test vet lint docker compose-up compose-down integration

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

docker:
	docker build -t kvrocks-leader:dev .

compose-up:
	docker compose -f deploy/docker-compose.yml up -d --build

compose-down:
	docker compose -f deploy/docker-compose.yml down -v

integration:
	bash deploy/integration.sh all
