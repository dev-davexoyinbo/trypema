.PHONY: \
	test redis-up redis-down \
	bench-local bench-redis bench \
	stress \
	stress-local stress-redis \
	stress-local-hot stress-local-uniform stress-local-burst \
	stress-local-uniform-matrix \
	stress-redis-hot stress-redis-skew \
	stress-redis-compare \
	stress-local-compare
	stress-help

REDIS_PORT ?= 16379
REDIS_URL ?= redis://127.0.0.1:$(REDIS_PORT)/

redis-up:
	@docker info >/dev/null 2>&1 || (echo "docker daemon not running" >&2; exit 1)
	@docker compose up -d redis
	@sh -c 'for i in $$(seq 1 60); do \
		if docker compose exec -T redis redis-cli ping >/dev/null 2>&1; then exit 0; fi; \
		sleep 0.25; \
	done; \
	echo "redis did not become ready in time" >&2; exit 1'

redis-down:
	@docker compose down -v --remove-orphans

test:
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo test --tests --features redis-tokio -- --show-output

bench-local:
	@cargo bench -p trypema --bench local_absolute
	@sleep 10
	@cargo bench -p trypema --bench local_suppressed

bench-redis:
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo bench -p trypema --features redis-tokio --bench redis_absolute; \
	sleep 10; \
	REDIS_URL="$(REDIS_URL)" cargo bench -p trypema --features redis-tokio --bench redis_suppressed

bench: bench-local bench-redis

stress-help:
	@cargo run -p trypema-stress -- --help

stress-local: stress-local-hot stress-local-uniform stress-local-burst

stress-redis: stress-redis-hot stress-redis-skew

stress: stress-local stress-redis

stress-local-hot:
	@cargo run --release -p trypema-stress -- \
		--provider local --strategy absolute --threads 16 \
		--key-dist hot --duration-s 30

stress-local-uniform:
	@cargo run --release -p trypema-stress -- \
		--provider local --strategy absolute --threads 16 \
		--key-dist uniform --key-space 100000 --duration-s 60

# Uniform distribution sweep across (key_space, rate_limit_per_s).
#
# Notes:
# - Runs in max-throughput mode (no pacing).
# - For `trypema` we run both `absolute` and `suppressed` strategies.
# - For `burster` and `governor`, `--strategy suppressed` is ignored by the harness.
#
# Override any of these from your shell, e.g.
#   STRESS_UNIFORM_KEY_SPACES='1 10 100' STRESS_UNIFORM_RATES='64 128 256' make stress-local-uniform-matrix
STRESS_UNIFORM_DURATION_S ?= 30
STRESS_UNIFORM_THREADS ?= 16
STRESS_UNIFORM_WINDOW_S ?= 10
STRESS_UNIFORM_GROUP_MS ?= 10
STRESS_UNIFORM_KEY_SPACES ?= 10 1000 10000
STRESS_UNIFORM_RATES ?= 1 10 100 10000 100000

stress-local-uniform-matrix:
	@set -e; \
	for ks in $(STRESS_UNIFORM_KEY_SPACES); do \
	  for r in $(STRESS_UNIFORM_RATES); do \
	    echo "== local trypema absolute: key_space=$$ks rate_limit_per_s=$$r =="; \
	    cargo run --release -p trypema-stress -- \
	      --provider local --local-limiter trypema --strategy absolute --threads $(STRESS_UNIFORM_THREADS) \
	      --window-s $(STRESS_UNIFORM_WINDOW_S) --group-ms $(STRESS_UNIFORM_GROUP_MS) \
	      --key-dist uniform --key-space $$ks --rate-limit-per-s $$r \
	      --mode max \
	      --duration-s $(STRESS_UNIFORM_DURATION_S); \
	    echo "== local trypema suppressed: key_space=$$ks rate_limit_per_s=$$r =="; \
	    cargo run --release -p trypema-stress -- \
	      --provider local --local-limiter trypema --strategy suppressed --threads $(STRESS_UNIFORM_THREADS) \
	      --window-s $(STRESS_UNIFORM_WINDOW_S) --group-ms $(STRESS_UNIFORM_GROUP_MS) \
	      --key-dist uniform --key-space $$ks --rate-limit-per-s $$r \
	      --mode max \
	      --duration-s $(STRESS_UNIFORM_DURATION_S); \
	    echo "== local burster absolute: key_space=$$ks rate_limit_per_s=$$r =="; \
	    cargo run --release -p trypema-stress -- \
	      --provider local --local-limiter burster --strategy absolute --threads $(STRESS_UNIFORM_THREADS) \
	      --window-s $(STRESS_UNIFORM_WINDOW_S) --group-ms $(STRESS_UNIFORM_GROUP_MS) \
	      --key-dist uniform --key-space $$ks --rate-limit-per-s $$r \
	      --mode max \
	      --duration-s $(STRESS_UNIFORM_DURATION_S); \
	    echo "== local governor absolute: key_space=$$ks rate_limit_per_s=$$r =="; \
	    cargo run --release -p trypema-stress -- \
	      --provider local --local-limiter governor --strategy absolute --threads $(STRESS_UNIFORM_THREADS) \
	      --window-s $(STRESS_UNIFORM_WINDOW_S) --group-ms $(STRESS_UNIFORM_GROUP_MS) \
	      --key-dist uniform --key-space $$ks --rate-limit-per-s $$r \
	      --mode max \
	      --duration-s $(STRESS_UNIFORM_DURATION_S); \
	  done; \
	done

stress-local-burst:
	@cargo run --release -p trypema-stress -- \
		--provider local --strategy suppressed --threads 16 \
		--key-dist skewed --key-space 100000 --hot-fraction 0.8 \
		--mode target-qps --target-qps 20000 --burst-qps 200000 \
		--burst-period-ms 30000 --burst-duration-ms 5000 \
		--duration-s 120

stress-redis-hot:
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 \
		--key-dist hot --duration-s 60 --redis-url "$(REDIS_URL)" --redis-prefix stress

stress-redis-skew:
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy suppressed --threads 16 \
		--key-dist skewed --key-space 100000 --hot-fraction 0.8 \
		--duration-s 120 --redis-url "$(REDIS_URL)" --redis-prefix stress

stress-redis-compare:
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000 --redis-limiter trypema; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy suppressed --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000 --redis-limiter trypema; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000 --redis-limiter cell --cell-burst 15; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000 --redis-limiter gcra --gcra-burst 15; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000 --redis-limiter trypema; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy suppressed --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000 --redis-limiter trypema; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000 --redis-limiter cell --cell-burst 1000000; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000 --redis-limiter gcra --gcra-burst 1000000

stress-local-compare:
	@set -e; \
	cargo run --release -p trypema-stress -- \
		--provider local --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--rate-limit-per-s 1000 --window-s 10 --local-limiter burster; \
	sleep 5; \
	cargo run --release -p trypema-stress -- \
		--provider local --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--rate-limit-per-s 1000 --window-s 10 --local-limiter governor; \
	sleep 5; \
	cargo run --release -p trypema-stress -- \
		--provider local --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--rate-limit-per-s 1000 --window-s 10 --local-limiter trypema; \
	sleep 5; \
	cargo run --release -p trypema-stress -- \
		--provider local --strategy suppressed --threads 16 --key-dist hot --duration-s 30 \
		--rate-limit-per-s 1000 --window-s 10 --local-limiter trypema; \
	sleep 5; \
	cargo run --release -p trypema-stress -- \
		--provider local --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--rate-limit-per-s 1000000000 --window-s 10 --local-limiter burster; \
	sleep 5; \
	cargo run --release -p trypema-stress -- \
		--provider local --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--rate-limit-per-s 1000000000 --window-s 10 --local-limiter governor; \
	sleep 5; \
	cargo run --release -p trypema-stress -- \
		--provider local --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--rate-limit-per-s 1000000000 --window-s 10 --local-limiter trypema; \
	sleep 5; \
	cargo run --release -p trypema-stress -- \
		--provider local --strategy suppressed --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--rate-limit-per-s 1000000000 --window-s 10 --local-limiter trypema 
