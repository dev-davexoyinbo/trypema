.PHONY: test-redis test redis-up redis-down

REDIS_PORT ?= 16379
REDIS_URL ?= redis://127.0.0.1:$(REDIS_PORT)

redis-up:
	@docker compose up -d redis
	@sh -c 'for i in $$(seq 1 60); do \
		if docker compose exec -T redis redis-cli ping >/dev/null 2>&1; then exit 0; fi; \
		sleep 0.25; \
	done; \
	echo "redis did not become ready in time" >&2; exit 1'

redis-down:
	@docker compose down -v --remove-orphans

test-redis:
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo test --features redis-tokio

test: test-redis
