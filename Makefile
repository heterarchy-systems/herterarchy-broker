DOCKER ?= docker
DOCKER_IMAGE ?= herterarchy-broker:local
DOCKER_VERSION ?= dev
DOCKER_TAG ?= dev
DOCKER_PLATFORMS ?= linux/amd64,linux/arm64
DOCKER_BUILDER ?= herterarchy-broker-builder
DOCKERHUB_REPOSITORY ?= herterarchy-broker
DOCKER_VCS_REF := $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
DOCKER_CLUSTER_ADDRESSES ?= 127.0.0.1:8811 127.0.0.1:8812 127.0.0.1:8813
DOCKER_CLUSTER_OPERATIONS_ADDRESSES ?= 127.0.0.1:9811 127.0.0.1:9812 127.0.0.1:9813
DOCKER_CLUSTER_E2E_PROJECT ?= herterarchy-broker-e2e
DOCKER_CLUSTER_TLS_DIR ?= $(CURDIR)/target/docker-cluster-raft-tls
DOCKER_CLUSTER_E2E_NODE1_PORT ?= 18821
DOCKER_CLUSTER_E2E_NODE2_PORT ?= 18822
DOCKER_CLUSTER_E2E_NODE3_PORT ?= 18823
DOCKER_CLUSTER_E2E_NODE1_OPERATIONS_PORT ?= 18831
DOCKER_CLUSTER_E2E_NODE2_OPERATIONS_PORT ?= 18832
DOCKER_CLUSTER_E2E_NODE3_OPERATIONS_PORT ?= 18833
DOCKER_CLUSTER_E2E_ADDRESSES := 127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE1_PORT) 127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE2_PORT) 127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE3_PORT)
DOCKER_CLUSTER_E2E_OPERATIONS_ADDRESSES := 127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE1_OPERATIONS_PORT) 127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE2_OPERATIONS_PORT) 127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE3_OPERATIONS_PORT)

.PHONY: rust_process_e2e cutover perf_check process_e2e extended doctor ci \
	docker_build docker_compose_config docker_up docker_down docker_cluster_config \
	docker_cluster_up docker_cluster_down docker_cluster_smoke docker_cluster_failover_probe \
	docker_cluster_rejoin_probe docker_cluster_stop_node1 docker_cluster_recreate_node1 \
	docker_cluster_e2e docker_builder docker_multiarch_check docker_hub_push

rust_process_e2e:
	cargo test -p agent-broker-runtime --test process_restart_e2e -- --nocapture
	cargo test -p agent-broker-runtime --test tcp_runtime_e2e -- --nocapture

cutover:
	cargo xtask cutover

perf_check:
	cargo xtask perf

process_e2e: rust_process_e2e

extended:
	cargo xtask extended

doctor:
	cargo run -p agent-broker-runtime --bin agentbrokerd -- doctor --nodes 1
	cargo run -p agent-broker-runtime --bin agentbrokerd -- doctor --nodes 3

docker_build:
	$(DOCKER) build \
		--build-arg VERSION=$(DOCKER_VERSION) \
		--build-arg VCS_REF=$(DOCKER_VCS_REF) \
		-t $(DOCKER_IMAGE) .

docker_compose_config:
	AGENT_BROKER_IMAGE=$(DOCKER_IMAGE) \
	AGENT_BROKER_VERSION=$(DOCKER_VERSION) \
	AGENT_BROKER_VCS_REF=$(DOCKER_VCS_REF) \
		$(DOCKER) compose config

docker_up:
	AGENT_BROKER_IMAGE=$(DOCKER_IMAGE) \
	AGENT_BROKER_VERSION=$(DOCKER_VERSION) \
	AGENT_BROKER_VCS_REF=$(DOCKER_VCS_REF) \
		$(DOCKER) compose up -d --build

docker_down:
	$(DOCKER) compose down

docker_cluster_config:
	@mkdir -p $(DOCKER_CLUSTER_TLS_DIR)
	@test -f $(DOCKER_CLUSTER_TLS_DIR)/ca.pem || cargo run -q -p agent-broker-consensus --example generate_cluster_tls -- $(DOCKER_CLUSTER_TLS_DIR) 1 2 3
	AGENT_BROKER_IMAGE=$(DOCKER_IMAGE) \
	AGENT_BROKER_VERSION=$(DOCKER_VERSION) \
	AGENT_BROKER_VCS_REF=$(DOCKER_VCS_REF) \
	AGENT_BROKER_RAFT_TLS_DIR=$(DOCKER_CLUSTER_TLS_DIR) \
		$(DOCKER) compose -f compose.cluster.yaml config

docker_cluster_up:
	@mkdir -p $(DOCKER_CLUSTER_TLS_DIR)
	@test -f $(DOCKER_CLUSTER_TLS_DIR)/ca.pem || cargo run -q -p agent-broker-consensus --example generate_cluster_tls -- $(DOCKER_CLUSTER_TLS_DIR) 1 2 3
	AGENT_BROKER_IMAGE=$(DOCKER_IMAGE) \
	AGENT_BROKER_VERSION=$(DOCKER_VERSION) \
	AGENT_BROKER_VCS_REF=$(DOCKER_VCS_REF) \
	AGENT_BROKER_RAFT_TLS_DIR=$(DOCKER_CLUSTER_TLS_DIR) \
		$(DOCKER) compose -f compose.cluster.yaml up -d --build

docker_cluster_down:
	AGENT_BROKER_RAFT_TLS_DIR=$(DOCKER_CLUSTER_TLS_DIR) $(DOCKER) compose -f compose.cluster.yaml down

docker_cluster_smoke:
	cargo run -q -p agent-broker-runtime --example cluster_probe -- \
		docker-cluster-before-failover 3 $(DOCKER_CLUSTER_ADDRESSES)

docker_cluster_failover_probe:
	cargo run -q -p agent-broker-runtime --example cluster_probe -- \
		docker-cluster-after-failover 2 $(DOCKER_CLUSTER_ADDRESSES)

docker_cluster_rejoin_probe:
	cargo run -q -p agent-broker-runtime --example cluster_probe -- \
		docker-cluster-after-rejoin 3 $(DOCKER_CLUSTER_ADDRESSES)

docker_cluster_stop_node1:
	AGENT_BROKER_RAFT_TLS_DIR=$(DOCKER_CLUSTER_TLS_DIR) $(DOCKER) compose -f compose.cluster.yaml stop agent-broker-1

docker_cluster_recreate_node1:
	@mkdir -p $(DOCKER_CLUSTER_TLS_DIR)
	@test -f $(DOCKER_CLUSTER_TLS_DIR)/ca.pem || cargo run -q -p agent-broker-consensus --example generate_cluster_tls -- $(DOCKER_CLUSTER_TLS_DIR) 1 2 3
	AGENT_BROKER_IMAGE=$(DOCKER_IMAGE) \
	AGENT_BROKER_VERSION=$(DOCKER_VERSION) \
	AGENT_BROKER_VCS_REF=$(DOCKER_VCS_REF) \
	AGENT_BROKER_RAFT_TLS_DIR=$(DOCKER_CLUSTER_TLS_DIR) \
		$(DOCKER) compose -f compose.cluster.yaml up -d --build --no-deps --force-recreate agent-broker-1

docker_cluster_e2e:
	@set -eu; \
	mkdir -p $(CURDIR)/target; \
	tls_dir="$$(mktemp -d $(CURDIR)/target/docker-cluster-e2e-tls.XXXXXX)"; \
	cargo run -q -p agent-broker-consensus --example generate_cluster_tls -- "$$tls_dir" 1 2 3; \
	cleanup() { \
		AGENT_BROKER_RAFT_TLS_DIR="$$tls_dir" \
		AGENT_BROKER_NODE1_PORT=$(DOCKER_CLUSTER_E2E_NODE1_PORT) \
		AGENT_BROKER_NODE2_PORT=$(DOCKER_CLUSTER_E2E_NODE2_PORT) \
		AGENT_BROKER_NODE3_PORT=$(DOCKER_CLUSTER_E2E_NODE3_PORT) \
		AGENT_BROKER_NODE1_OPERATIONS_PORT=$(DOCKER_CLUSTER_E2E_NODE1_OPERATIONS_PORT) \
		AGENT_BROKER_NODE2_OPERATIONS_PORT=$(DOCKER_CLUSTER_E2E_NODE2_OPERATIONS_PORT) \
		AGENT_BROKER_NODE3_OPERATIONS_PORT=$(DOCKER_CLUSTER_E2E_NODE3_OPERATIONS_PORT) \
			$(DOCKER) compose -p $(DOCKER_CLUSTER_E2E_PROJECT) -f compose.cluster.yaml down -v --remove-orphans >/dev/null 2>&1 || true; \
	}; \
	final_cleanup() { \
		cleanup; \
		rm -rf "$$tls_dir"; \
	}; \
	start_cluster() { \
		AGENT_BROKER_IMAGE=$(DOCKER_IMAGE) \
		AGENT_BROKER_VERSION=$(DOCKER_VERSION) \
		AGENT_BROKER_VCS_REF=$(DOCKER_VCS_REF) \
		AGENT_BROKER_RAFT_TLS_DIR="$$tls_dir" \
		AGENT_BROKER_NODE1_PORT=$(DOCKER_CLUSTER_E2E_NODE1_PORT) \
		AGENT_BROKER_NODE2_PORT=$(DOCKER_CLUSTER_E2E_NODE2_PORT) \
		AGENT_BROKER_NODE3_PORT=$(DOCKER_CLUSTER_E2E_NODE3_PORT) \
		AGENT_BROKER_NODE1_OPERATIONS_PORT=$(DOCKER_CLUSTER_E2E_NODE1_OPERATIONS_PORT) \
		AGENT_BROKER_NODE2_OPERATIONS_PORT=$(DOCKER_CLUSTER_E2E_NODE2_OPERATIONS_PORT) \
		AGENT_BROKER_NODE3_OPERATIONS_PORT=$(DOCKER_CLUSTER_E2E_NODE3_OPERATIONS_PORT) \
			$(DOCKER) compose -p $(DOCKER_CLUSTER_E2E_PROJECT) -f compose.cluster.yaml up -d --build; \
	}; \
	trap final_cleanup EXIT INT TERM; \
	cleanup; \
	start_cluster; \
	cargo run -q -p agent-broker-runtime --example cluster_probe -- \
		docker-cluster-fresh 3 $(DOCKER_CLUSTER_E2E_ADDRESSES); \
	cargo run -q -p agent-broker-runtime --example operations_probe -- \
		assert-one-ready $(DOCKER_CLUSTER_E2E_OPERATIONS_ADDRESSES); \
	AGENT_BROKER_RAFT_TLS_DIR="$$tls_dir" $(DOCKER) compose -p $(DOCKER_CLUSTER_E2E_PROJECT) -f compose.cluster.yaml stop agent-broker-1; \
	cargo run -q -p agent-broker-runtime --example cluster_probe -- \
		docker-cluster-failover 2 $(DOCKER_CLUSTER_E2E_ADDRESSES); \
	cargo run -q -p agent-broker-runtime --example operations_probe -- \
		assert-one-ready \
		127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE2_OPERATIONS_PORT) \
		127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE3_OPERATIONS_PORT); \
	AGENT_BROKER_IMAGE=$(DOCKER_IMAGE) \
	AGENT_BROKER_VERSION=$(DOCKER_VERSION) \
	AGENT_BROKER_VCS_REF=$(DOCKER_VCS_REF) \
	AGENT_BROKER_RAFT_TLS_DIR="$$tls_dir" \
	AGENT_BROKER_NODE1_PORT=$(DOCKER_CLUSTER_E2E_NODE1_PORT) \
	AGENT_BROKER_NODE2_PORT=$(DOCKER_CLUSTER_E2E_NODE2_PORT) \
	AGENT_BROKER_NODE3_PORT=$(DOCKER_CLUSTER_E2E_NODE3_PORT) \
	AGENT_BROKER_NODE1_OPERATIONS_PORT=$(DOCKER_CLUSTER_E2E_NODE1_OPERATIONS_PORT) \
	AGENT_BROKER_NODE2_OPERATIONS_PORT=$(DOCKER_CLUSTER_E2E_NODE2_OPERATIONS_PORT) \
	AGENT_BROKER_NODE3_OPERATIONS_PORT=$(DOCKER_CLUSTER_E2E_NODE3_OPERATIONS_PORT) \
		$(DOCKER) compose -p $(DOCKER_CLUSTER_E2E_PROJECT) -f compose.cluster.yaml up -d --no-deps --force-recreate agent-broker-1; \
	cargo run -q -p agent-broker-runtime --example cluster_probe -- \
		docker-cluster-rejoin 3 $(DOCKER_CLUSTER_E2E_ADDRESSES); \
	cargo run -q -p agent-broker-runtime --example operations_probe -- \
		assert-one-ready $(DOCKER_CLUSTER_E2E_OPERATIONS_ADDRESSES); \
	cleanup; \
	start_cluster; \
	partition_fresh_json="$$(cargo run -q -p agent-broker-runtime --example cluster_probe -- \
		docker-cluster-partition-fresh 3 $(DOCKER_CLUSTER_E2E_ADDRESSES))"; \
	printf '%s\n' "$$partition_fresh_json"; \
	cargo run -q -p agent-broker-runtime --example operations_probe -- \
		assert-one-ready $(DOCKER_CLUSTER_E2E_OPERATIONS_ADDRESSES); \
	fresh_term="$$(printf '%s\n' "$$partition_fresh_json" | sed -E 's/.*"term":([0-9]+).*/\1/')"; \
	test -n "$$fresh_term"; \
	node1_container="$$( AGENT_BROKER_RAFT_TLS_DIR="$$tls_dir" $(DOCKER) compose -p $(DOCKER_CLUSTER_E2E_PROJECT) -f compose.cluster.yaml ps -q agent-broker-1 )"; \
	test -n "$$node1_container"; \
	raft_network="$(DOCKER_CLUSTER_E2E_PROJECT)_raft"; \
	$(DOCKER) network disconnect "$$raft_network" "$$node1_container"; \
	cargo run -q -p agent-broker-runtime --example operations_probe -- \
		assert-not-ready 127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE1_OPERATIONS_PORT); \
	cargo run -q -p agent-broker-runtime --example cluster_probe -- \
		expect-rejected docker-cluster-stale-write 127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE1_PORT); \
	majority_json="$$(cargo run -q -p agent-broker-runtime --example cluster_probe -- \
		docker-cluster-partition-majority 2 \
		127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE2_PORT) \
		127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE3_PORT))"; \
	printf '%s\n' "$$majority_json"; \
	majority_term="$$(printf '%s\n' "$$majority_json" | sed -E 's/.*"term":([0-9]+).*/\1/')"; \
	majority_revision="$$(printf '%s\n' "$$majority_json" | sed -E 's/.*"revision":([0-9]+).*/\1/')"; \
	test -n "$$majority_term"; \
	test -n "$$majority_revision"; \
	term_delta="$$(($$majority_term - $$fresh_term))"; \
	test "$$term_delta" -ge 1; \
	test "$$term_delta" -le 5; \
	cargo run -q -p agent-broker-runtime --example operations_probe -- \
		assert-one-ready \
		127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE2_OPERATIONS_PORT) \
		127.0.0.1:$(DOCKER_CLUSTER_E2E_NODE3_OPERATIONS_PORT); \
	$(DOCKER) network connect --ip 10.77.0.11 --alias raft-node-1 "$$raft_network" "$$node1_container"; \
	cargo run -q -p agent-broker-runtime --example cluster_probe -- \
		assert-exact "$$majority_term" "$$majority_revision" 3 $(DOCKER_CLUSTER_E2E_ADDRESSES); \
	cargo run -q -p agent-broker-runtime --example operations_probe -- \
		assert-one-ready $(DOCKER_CLUSTER_E2E_OPERATIONS_ADDRESSES)

docker_builder:
	@if ! $(DOCKER) buildx inspect $(DOCKER_BUILDER) >/dev/null 2>&1; then \
		$(DOCKER) buildx create --name $(DOCKER_BUILDER) --driver docker-container >/dev/null; \
	fi
	@$(DOCKER) buildx inspect $(DOCKER_BUILDER) --bootstrap >/dev/null

docker_multiarch_check: docker_builder
	$(DOCKER) buildx build \
		--builder $(DOCKER_BUILDER) \
		--platform $(DOCKER_PLATFORMS) \
		--build-arg VERSION=$(DOCKER_TAG) \
		--build-arg VCS_REF=$(DOCKER_VCS_REF) \
		--provenance=mode=max \
		--sbom=true .

docker_hub_push: docker_builder
	@test -n "$(DOCKERHUB_NAMESPACE)" || (echo "DOCKERHUB_NAMESPACE is required" >&2; exit 2)
	$(DOCKER) buildx build \
		--builder $(DOCKER_BUILDER) \
		--platform $(DOCKER_PLATFORMS) \
		--build-arg VERSION=$(DOCKER_TAG) \
		--build-arg VCS_REF=$(DOCKER_VCS_REF) \
		-t $(DOCKERHUB_NAMESPACE)/$(DOCKERHUB_REPOSITORY):$(DOCKER_TAG) \
		--provenance=mode=max \
		--sbom=true \
		--push .

ci: cutover rust_process_e2e
