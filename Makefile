# One-command entry points. `make help` lists them.
PYTHON ?= .venv/bin/python
TARGET ?= dev
PROFILE ?= fmcg-free
# The dev target's catalog. The demo data is synthetic, so deploy it to dev only.
CATALOG ?= fmcg_dev
ROWS ?= 5000000
# Spark 4 needs Java 17+. Homebrew's JDK is not on macOS's java_home list, so point at it directly.
JAVA_HOME ?= $(shell ls -d /opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home 2>/dev/null || ls -d /usr/lib/jvm/java-21-openjdk-* 2>/dev/null | head -1)
export JAVA_HOME

.PHONY: help venv test test-spark test-kafka kafka-up kafka-down kafka-produce entity-eval benchmark demo-data deploy

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/ -/'

venv: ## create .venv with everything needed to test and benchmark locally
	python3 -m venv .venv
	.venv/bin/pip install -q -r requirements-dev.txt -r requirements-spark.txt

test: ## fast tests (no Spark): contracts, bundle config, entity matching, SCD2 reference
	$(PYTHON) -m pytest -q -m "not spark"

test-spark: ## run the real notebooks on local Spark + Delta against synthetic data (~10 min)
	$(PYTHON) -m pytest -q tests/spark

kafka-up: ## start a local single-node Kafka (needs Docker running; see docker-compose.kafka.yml)
	docker compose -f docker-compose.kafka.yml up -d

kafka-down: ## stop the local Kafka and delete its data
	docker compose -f docker-compose.kafka.yml down -v

kafka-produce: ## replay synthetic orders into the local broker's `orders` topic
	$(PYTHON) scripts/produce_orders_to_kafka.py --bootstrap localhost:9092 --topic orders

test-kafka: ## the Kafka source against the local broker (never run yet; run make kafka-up first)
	KAFKA_BOOTSTRAP_SERVERS=localhost:9092 $(PYTHON) -m pytest -q tests/spark/test_kafka_broker.py

entity-eval: ## precision/recall of entity matching on unseen synthetic seeds
	$(PYTHON) benchmarks/evaluate_entity_resolution.py

benchmark: ## local scale benchmark; ROWS=5000000 by default
	$(PYTHON) benchmarks/run_benchmark.py --rows $(ROWS) --output benchmarks/results/local-$(ROWS).json

demo-data: ## generate the synthetic landing zone in demo_data/
	$(PYTHON) scripts/generate_demo_data.py demo_data

deploy: demo-data ## validate, deploy and run the synthetic merger demo on a Databricks workspace (dev only)
	databricks bundle validate --target $(TARGET)
	databricks bundle deploy --target $(TARGET)
	databricks bundle run --target $(TARGET) fmcg_prepare_storage
	./scripts/upload_demo_data.sh $(PROFILE) $(CATALOG) demo_data
	databricks bundle run --target $(TARGET) fmcg_demo_seed_parent
	databricks bundle run --target $(TARGET) fmcg_initial_load
	databricks bundle run --target $(TARGET) fmcg_entity_resolution
