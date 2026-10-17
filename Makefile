# Use COMPOSE="podman compose" for podman.
COMPOSE ?= docker compose

.PHONY: up down before after

up:
	$(COMPOSE) up -d
	@sleep 5

down:
	$(COMPOSE) down -v

# nats.go v1.54.0 (bug)
before:
	go run .

# nats.go PR #2193 head (fix)
after:
	go run -modfile=go.fixed.mod .
