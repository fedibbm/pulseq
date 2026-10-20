#=============================================================================
#  Makefile — PulseQ message broker
#  Targets: make help | verify | build | package | server | server-pg
#           docker-up | docker-down | docker-restart | docker-logs
#           postgres-up | postgres-down | postgres-down-vol
#           dashboard-install | dashboard-build | dashboard-dev
#           demo-embedded | demo-network | demo-reconnect
#           health | metrics | publish | dlq | dlq-replay | clean
#=============================================================================

M    := mvn
NPM  := npm
URL  := http://localhost:8080

# Publish / DLQ helpers take TOPIC= and PAYLOAD= (defaults shown below)
TOPIC   ?= orders
PAYLOAD ?= hello
MAXRET  ?= 3
TTL     ?= 0

.PHONY: help verify test build package clean \
        server server-pg \
        postgres-up postgres-down postgres-down-vol \
        docker-up docker-down docker-restart docker-logs \
        dashboard-install dashboard-build dashboard-dev \
        demo-embedded demo-network demo-reconnect \
        health metrics publish dlq dlq-replay

help: ## List available targets
	@echo "PulseQ make targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk -F':.*?## ' '{printf "  %-22s %s\n", $$1, $$2}'

#----------------------------- Build / test ----------------------------------
verify: ## Run the full build + test suite (mvn clean verify)
	$(M) clean verify

test: ## Run tests (unit + E2E; Postgres integration tests self-skip)
	$(M) test

build: ## Compile all modules without tests
	$(M) -DskipTests package

package: ## Package jars (required before a Docker build)
	$(M) package

clean: ## Clean all Maven build output
	$(M) clean

#------------------------------- Server --------------------------------------
server: ## Run the standalone server (in-memory store) on :8080
	$(M) spring-boot:run -pl pulseq-server

server-pg: ## Run the server with the PostgreSQL store (needs: make postgres-up)
	$(M) spring-boot:run -pl pulseq-server \
	  -Dspring-boot.run.arguments="--pulseq.store=postgres"

#---------------------------- PostgreSQL -------------------------------------
postgres-up: ## Start the local PostgreSQL container
	docker compose up -d postgres

postgres-down: ## Stop PostgreSQL (volume kept)
	docker compose stop postgres

postgres-down-vol: ## Stop PostgreSQL and delete its data volume
	docker compose rm -sfv postgres

#------------------------------- Docker --------------------------------------
docker-up: ## Build + start the full stack (PostgreSQL + server + dashboard)
	$(M) package
	docker compose up -d --build

docker-down: ## Stop the stack (database volume kept)
	docker compose down

docker-down-vol: ## Stop the stack AND delete the database
	docker compose down -v

docker-restart: ## Restart only the server container
	docker compose restart server

docker-logs: ## Tail the server logs
	docker compose logs -f server

#----------------------------- Dashboard -------------------------------------
dashboard-install: ## Install the dashboard's npm dependencies
	$(NPM) --prefix pulseq-dashboard install

dashboard-build: ## Production build of the Angular dashboard
	$(NPM) --prefix pulseq-dashboard run build

dashboard-dev: ## Angular dev server with live reload + API proxy (needs: make server)
	cd pulseq-dashboard && npx ng serve --proxy-config proxy.conf.json

#------------------------------- Demos ---------------------------------------
demo-embedded: ## Demo 1 — embedded broker, 5 core scenarios (no server needed)
	$(M) exec:java -pl pulseq-sdk -Dexec.mainClass=com.pulseq.sdk.SdkDemo

demo-network: ## Demo 2 — network mode against a live server (needs: make server)
	$(M) exec:java -pl pulseq-sdk -Dexec.mainClass=com.pulseq.sdk.NetworkDemo

demo-reconnect: ## Demo 3 — client resilience across a server restart (needs: make server)
	$(M) exec:java -pl pulseq-sdk -Dexec.mainClass=com.pulseq.sdk.ReconnectDemo

#----------------------------- API / observability ----------------------------
health: ## GET /health
	curl -s $(URL)/health

metrics: ## GET /metrics
	curl -s $(URL)/metrics

publish: ## POST /publish/{TOPIC}  (make publish TOPIC=news PAYLOAD=hi)
	curl -s -X POST $(URL)/publish/$(TOPIC) \
	  -H "Content-Type: application/json" \
	  -d '{"payload":"$(PAYLOAD)","maxRetries":$(MAXRET),"ttlMillis":$(TTL)}'

dlq: ## GET /dlq/{TOPIC}
	curl -s $(URL)/dlq/$(TOPIC)

dlq-replay: ## POST /dlq/{TOPIC}/replay
	curl -s -X POST $(URL)/dlq/$(TOPIC)/replay
