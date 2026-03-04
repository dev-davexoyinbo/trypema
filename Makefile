.PHONY: \
	test-redis test-redis-tokio test-redis-smol test redis-up redis-down \
	bench-local bench-redis bench-redis-tokio bench-redis-smol bench \
	sanity sanity-noredis sanity-redis-tokio sanity-redis-smol \
	stress \
	stress-local stress-redis stress-hybrid \
	stress-local-hot stress-local-uniform stress-local-burst \
	stress-local-uniform-matrix \
	stress-redis-hot stress-redis-hot-tokio stress-redis-hot-smol \
	stress-redis-skew stress-redis-skew-tokio stress-redis-skew-smol \
	stress-hybrid-hot stress-hybrid-hot-tokio stress-hybrid-hot-smol \
	stress-hybrid-skew stress-hybrid-skew-tokio stress-hybrid-skew-smol \
	stress-redis-compare stress-redis-compare-tokio stress-redis-compare-smol \
	stress-local-compare \
	stress-help help

REDIS_PORT ?= 16379
REDIS_URL ?= redis://127.0.0.1:$(REDIS_PORT)/

COLOR_GREEN := \033[32m
COLOR_BLUE := \033[34m
COLOR_RESET := \033[0m

help: ## Show available commands
	@printf "$(COLOR_BLUE)Available commands:$(COLOR_RESET)\n"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_.-]+:.*##/ {printf "%s\t%s\n", $$1, $$2}' $(MAKEFILE_LIST) \
					| sort \
					| awk -F"\t" -v green="$(COLOR_GREEN)" -v blue="$(COLOR_BLUE)" -v reset="$(COLOR_RESET)" 'function line(prefix){printf "%s-------------------------------- %s --------------------------------%s\n", blue, prefix, reset} function order(p){return (p=="misc"?0:(p=="dev"?1:(p=="local"?2:(p=="test"?3:(p=="prod"?4:99)))))} {cmd=$$1; desc=$$2; prefix=cmd; if(index(cmd,"-")>0){sub(/-.*/,"",prefix)} else {prefix="misc"}; groups[prefix]=groups[prefix] sprintf("%s%-24s%s %s\n", green, cmd, reset, desc)} END{n=0; for(p in groups){keys[n++]=p}; for(i=0;i<n;i++){for(j=i+1;j<n;j++){if(order(keys[j])<order(keys[i]) || (order(keys[j])==order(keys[i]) && keys[j]<keys[i])){t=keys[i];keys[i]=keys[j];keys[j]=t}}}; for(i=0;i<n;i++){p=keys[i]; line(p); printf "%s", groups[p]}}'
	@echo
	@echo "Tip: use 'make <target>' (e.g. 'make dev-up')"

redis-up: ## Start local redis (docker compose)
	@docker info >/dev/null 2>&1 || (echo "docker daemon not running" >&2; exit 1)
	@REDIS_PORT="$(REDIS_PORT)" docker compose up -d redis
	@sh -c 'for i in $$(seq 1 60); do \
		if docker compose exec -T redis redis-cli ping >/dev/null 2>&1; then exit 0; fi; \
		sleep 0.25; \
	done; \
	echo "redis did not become ready in time" >&2; exit 1'

redis-down: ## Stop local redis and remove volumes
	@docker compose down -v --remove-orphans

test-redis: test-redis-tokio test-redis-smol ## Run Redis-backed tests (tokio + smol)

test-redis-tokio: ## Run tests with Redis (tokio client)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo test -p trypema --features redis-tokio

test-redis-smol: ## Run tests with Redis (smol client)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo test -p trypema --features redis-smol

test: test-redis ## Run test suite (includes Redis tests)

# Sanity check: run tests + benches without Redis, then with redis-tokio, then with redis-smol.
sanity: sanity-noredis sanity-redis-tokio sanity-redis-smol ## Run tests + benches (no-redis, redis-tokio, redis-smol)

sanity-noredis: ## Run tests + local benches without Redis
	@cargo test -p trypema --no-default-features
	@cargo bench -p trypema --no-default-features --bench local_absolute
	@sleep 10
	@cargo bench -p trypema --no-default-features --bench local_suppressed

sanity-redis-tokio: ## Run tests + benches with Redis (tokio)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo test -p trypema --features redis-tokio; \
	cargo bench -p trypema --features redis-tokio --bench local_absolute; \
	sleep 10; \
	cargo bench -p trypema --features redis-tokio --bench local_suppressed; \
	sleep 10; \
	REDIS_URL="$(REDIS_URL)" cargo bench -p trypema --features redis-tokio --bench redis_absolute; \
	sleep 10; \
	REDIS_URL="$(REDIS_URL)" cargo bench -p trypema --features redis-tokio --bench redis_suppressed

sanity-redis-smol: ## Run tests + benches with Redis (smol)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo test -p trypema --features redis-smol; \
	cargo bench -p trypema --features redis-smol --bench local_absolute; \
	sleep 10; \
	cargo bench -p trypema --features redis-smol --bench local_suppressed; \
	sleep 10; \
	REDIS_URL="$(REDIS_URL)" cargo bench -p trypema --features redis-smol --bench redis_absolute; \
	sleep 10; \
	REDIS_URL="$(REDIS_URL)" cargo bench -p trypema --features redis-smol --bench redis_suppressed

bench-local: ## Run local benches (no Redis)
	@cargo bench -p trypema --bench local_absolute
	@sleep 10
	@cargo bench -p trypema --bench local_suppressed

bench-redis: ## Run Redis benches (tokio + smol)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" $(MAKE) -s bench-redis-tokio; \
	REDIS_URL="$(REDIS_URL)" $(MAKE) -s bench-redis-smol

bench-redis-tokio: ## Run Redis benches (tokio)
	@REDIS_URL="$(REDIS_URL)" cargo bench -p trypema --features redis-tokio --bench redis_absolute; \
	sleep 10; \
	REDIS_URL="$(REDIS_URL)" cargo bench -p trypema --features redis-tokio --bench redis_suppressed

bench-redis-smol: ## Run Redis benches (smol)
	@REDIS_URL="$(REDIS_URL)" cargo bench -p trypema --features redis-smol --bench redis_absolute; \
	sleep 10; \
	REDIS_URL="$(REDIS_URL)" cargo bench -p trypema --features redis-smol --bench redis_suppressed

bench: bench-local bench-redis ## Run all benches (local + redis)

stress-help: ## Show stress harness CLI help
	@cargo run -p trypema-stress -- --help

stress-local: stress-local-hot stress-local-uniform stress-local-burst ## Run local stress presets

stress-redis: stress-redis-hot stress-redis-skew ## Run Redis stress presets

stress-hybrid: stress-hybrid-hot stress-hybrid-skew ## Run hybrid (local+redis) stress presets

stress: stress-local stress-redis stress-hybrid ## Run all stress presets

stress-local-hot: ## Stress local provider (hot keys)
	@cargo run --release -p trypema-stress -- \
		--provider local --strategy absolute --threads 16 \
		--key-dist hot --duration-s 30

stress-local-uniform: ## Stress local provider (uniform keys)
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

stress-local-uniform-matrix: ## Sweep (key_space, rate_limit_per_s) locally
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

stress-local-burst: ## Stress local provider (bursty skewed keys)
	@cargo run --release -p trypema-stress -- \
		--provider local --strategy suppressed --threads 16 \
		--key-dist skewed --key-space 100000 --hot-fraction 0.8 \
		--mode target-qps --target-qps 20000 --burst-qps 200000 \
		--burst-period-ms 30000 --burst-duration-ms 5000 \
		--duration-s 120

stress-redis-hot: ## Stress Redis provider (hot keys)
	@$(MAKE) -s stress-redis-hot-tokio
	@$(MAKE) -s stress-redis-hot-smol

stress-redis-hot-tokio: ## Stress Redis provider (tokio, hot keys)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 \
		--key-dist hot --duration-s 60 --redis-url "$(REDIS_URL)" --redis-prefix stress

stress-redis-hot-smol: ## Stress Redis provider (smol, hot keys)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider redis --strategy absolute --threads 16 \
		--key-dist hot --duration-s 60 --redis-url "$(REDIS_URL)" --redis-prefix stress

stress-redis-skew: ## Stress Redis provider (skewed keys)
	@$(MAKE) -s stress-redis-skew-tokio
	@$(MAKE) -s stress-redis-skew-smol

stress-redis-skew-tokio: ## Stress Redis provider (tokio, skewed keys)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 \
		--key-dist skewed --key-space 100000 --hot-fraction 0.8 \
		--duration-s 120 --redis-url "$(REDIS_URL)" --redis-prefix stress

stress-redis-skew-smol: ## Stress Redis provider (smol, skewed keys)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider redis --strategy absolute --threads 16 \
		--key-dist skewed --key-space 100000 --hot-fraction 0.8 \
		--duration-s 120 --redis-url "$(REDIS_URL)" --redis-prefix stress

stress-hybrid-hot: ## Stress hybrid provider (hot keys)
	@$(MAKE) -s stress-hybrid-hot-tokio
	@$(MAKE) -s stress-hybrid-hot-smol

stress-hybrid-hot-tokio: ## Stress hybrid provider (tokio, hot keys)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider hybrid --strategy absolute --threads 16 \
		--key-dist hot --duration-s 60 --redis-url "$(REDIS_URL)" --redis-prefix stresshybrid

stress-hybrid-hot-smol: ## Stress hybrid provider (smol, hot keys)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider hybrid --strategy absolute --threads 16 \
		--key-dist hot --duration-s 60 --redis-url "$(REDIS_URL)" --redis-prefix stresshybrid

stress-hybrid-skew: ## Stress hybrid provider (skewed keys)
	@$(MAKE) -s stress-hybrid-skew-tokio
	@$(MAKE) -s stress-hybrid-skew-smol

stress-hybrid-skew-tokio: ## Stress hybrid provider (tokio, skewed keys)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider hybrid --strategy absolute --threads 16 \
		--key-dist skewed --key-space 100000 --hot-fraction 0.8 \
		--duration-s 120 --redis-url "$(REDIS_URL)" --redis-prefix stresshybrid

stress-hybrid-skew-smol: ## Stress hybrid provider (smol, skewed keys)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider hybrid --strategy absolute --threads 16 \
		--key-dist skewed --key-space 100000 --hot-fraction 0.8 \
		--duration-s 120 --redis-url "$(REDIS_URL)" --redis-prefix stresshybrid

stress-redis-compare: ## Compare Redis provider implementations (tokio + smol)
	@$(MAKE) -s stress-redis-compare-tokio
	@$(MAKE) -s stress-redis-compare-smol

stress-redis-compare-tokio: ## Compare Redis provider implementations (tokio)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	printf "\n\n== redis compare: trypema (redis provider) hot absolute rate=1000 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000 --redis-limiter trypema; \
	printf "\n\n== redis compare: trypema (hybrid provider) hot absolute rate=1000 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider hybrid --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000; \
	printf "\n\n== redis compare: trypema (redis provider) hot suppressed rate=1000 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy suppressed --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000 --redis-limiter trypema; \
	printf "\n\n== redis compare: redis-cell hot absolute rate=1000 burst=15 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000 --redis-limiter cell --cell-burst 15; \
	printf "\n\n== redis compare: gcra hot absolute rate=1000 burst=15 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000 --redis-limiter gcra --gcra-burst 15; \
	printf "\n\n== redis compare: trypema (redis provider) uniform(key_space=100000) absolute rate=1e9 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000 --redis-limiter trypema; \
	printf "\n\n== redis compare: trypema (hybrid provider) uniform(key_space=100000) absolute rate=1e9 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider hybrid --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000; \
	printf "\n\n== redis compare: trypema (redis provider) uniform(key_space=100000) suppressed rate=1e9 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy suppressed --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000 --redis-limiter trypema; \
	printf "\n\n== redis compare: redis-cell uniform(key_space=100000) absolute rate=1e9 burst=1e6 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000 --redis-limiter cell --cell-burst 1000000; \
	printf "\n\n== redis compare: gcra uniform(key_space=100000) absolute rate=1e9 burst=1e6 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-tokio -- \
		--provider redis --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000 --redis-limiter gcra --gcra-burst 1000000

stress-redis-compare-smol: ## Compare Redis provider implementations (smol)
	@set -e; \
	trap "$(MAKE) -s redis-down" EXIT; \
	$(MAKE) -s redis-up; \
	printf "\n\n== redis compare: trypema (redis provider) hot absolute rate=1000 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider redis --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000 --redis-limiter trypema; \
	printf "\n\n== redis compare: trypema (hybrid provider) hot absolute rate=1000 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider hybrid --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000; \
	printf "\n\n== redis compare: trypema (redis provider) hot suppressed rate=1000 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider redis --strategy suppressed --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000 --redis-limiter trypema; \
	printf "\n\n== redis compare: redis-cell hot absolute rate=1000 burst=15 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider redis --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000 --redis-limiter cell --cell-burst 15; \
	printf "\n\n== redis compare: gcra hot absolute rate=1000 burst=15 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider redis --strategy absolute --threads 16 --key-dist hot --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000 --redis-limiter gcra --gcra-burst 15; \
	printf "\n\n== redis compare: trypema (redis provider) uniform(key_space=100000) absolute rate=1e9 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider redis --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000 --redis-limiter trypema; \
	printf "\n\n== redis compare: trypema (hybrid provider) uniform(key_space=100000) absolute rate=1e9 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider hybrid --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000; \
	printf "\n\n== redis compare: trypema (redis provider) uniform(key_space=100000) suppressed rate=1e9 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider redis --strategy suppressed --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000 --redis-limiter trypema; \
	printf "\n\n== redis compare: redis-cell uniform(key_space=100000) absolute rate=1e9 burst=1e6 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider redis --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000 --redis-limiter cell --cell-burst 1000000; \
	printf "\n\n== redis compare: gcra uniform(key_space=100000) absolute rate=1e9 burst=1e6 ==\n"; \
	REDIS_URL="$(REDIS_URL)" cargo run --release -p trypema-stress --features redis-smol -- \
		--provider redis --strategy absolute --threads 16 --key-dist uniform --key-space 100000 --duration-s 30 \
		--redis-url "$(REDIS_URL)" --redis-prefix cmp --rate-limit-per-s 1000000000 --redis-limiter gcra --gcra-burst 1000000

stress-local-compare: ## Compare local limiters (burster/governor/trypema)
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
