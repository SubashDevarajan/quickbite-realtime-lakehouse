.DEFAULT_GOAL := help
COMPOSE := docker compose
TOOLS := $(COMPOSE) run --rm tools

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ------------------------------------------------------------------ lifecycle
up: ## Build images and start everything (Kafka, registry, generator, bronze/silver/gold, dashboard)
	$(COMPOSE) up -d --build
	@echo ""
	@echo "  Dashboard:  http://localhost:8501"
	@echo "  Kafka UI:   http://localhost:8080"
	@echo "  Registry:   http://localhost:8081/subjects"

down: ## Stop everything (data is kept)
	$(COMPOSE) down

clean: ## Stop everything and DELETE all data (Kafka topics, lakehouse, checkpoints)
	$(COMPOSE) down -v --remove-orphans

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Follow pipeline logs
	$(COMPOSE) logs -f --tail=50 bronze silver gold

logs-generator: ## Follow generator logs
	$(COMPOSE) logs -f --tail=50 generator

# ----------------------------------------------------------------- drills
verify: ## Stop the generator, wait for the pipeline to drain, check exactly-once against ground truth
	$(COMPOSE) stop generator
	$(TOOLS) python -m scripts.verify_exactly_once

resume-generator: ## Start producing again after `make verify`
	$(COMPOSE) start generator

chaos-kill-silver: ## SIGKILL the silver job mid-batch, restart it, and let it resume from its checkpoint
	$(COMPOSE) kill -s SIGKILL silver
	@echo "silver killed mid-flight. Restarting it; it will resume from its checkpoint (watch: make logs)"
	@sleep 5 && $(COMPOSE) up -d silver

chaos-pause-silver: ## Pause silver for 3 minutes to build a backlog, then watch it catch up
	$(COMPOSE) pause silver
	@echo "silver paused for 180s (bronze keeps ingesting)..." && sleep 180
	$(COMPOSE) unpause silver

replay: ## Rebuild silver + gold from bronze (deletes those tables + checkpoints)
	$(COMPOSE) stop silver gold
	$(TOOLS) python -m scripts.reset_layers silver gold
	$(COMPOSE) up -d silver gold

schema-check: ## Show the registry accepting v2 and rejecting a breaking v3 schema
	$(COMPOSE) run --rm --no-deps generator python -m scripts.check_schema_compat --url http://schema-registry:8081

maintain: ## OPTIMIZE + ZORDER + VACUUM all tables
	$(TOOLS) python -m pipeline.jobs.maintenance

benchmark: ## Restart the generator at 200 sessions/sec (~1,200 events/sec)
	SESSIONS_PER_SEC=200 $(COMPOSE) up -d --force-recreate generator

sql: ## Open a PySpark shell with the lakehouse tables registered as views
	$(COMPOSE) run --rm -it tools python -i -m scripts.lakehouse_shell

# ------------------------------------------------------------------ local dev
install-dev: ## Install dev dependencies into the current Python env (Java 17 required for Spark tests)
	pip install -r requirements/dev.txt

test: ## Run all tests (Spark tests need Java 17)
	pytest -q

test-fast: ## Run only the pure-Python tests (no Spark/Java)
	pytest -q -m "not spark"

lint: ## Lint with ruff
	ruff check .

.PHONY: help up down clean ps logs logs-generator verify resume-generator chaos-kill-silver \
	chaos-pause-silver replay schema-check maintain benchmark sql install-dev test test-fast lint
