up:
	docker compose up -d --build

produce-sample:
	docker compose exec -T -e PYTHONPATH=/opt/airflow/processing/streaming airflow-webserver python /opt/airflow/replay/producer_account_applications.py $(ARGS)

stream:
	bash scripts/run_streaming_job.sh $(ARGS)

test-streaming:
	docker compose exec -T -e PYTHONPATH=/opt/airflow/processing/streaming:/opt/airflow/replay airflow-webserver python -m pytest -q /opt/airflow/project-tests/test_streaming.py -p no:cacheprovider

down:
	docker compose down

down-volumes:
	docker compose down -v

logs:
	docker compose logs -f

ps:
	docker compose ps

airflow:
	open http://localhost:8080

spark:
	open http://localhost:8081

create-topic:
	docker compose exec kafka kafka-topics \
		--bootstrap-server kafka:29092 \
		--create \
		--if-not-exists \
		--topic account-applications \
		--partitions 3 \
		--replication-factor 1

list-topics:
	docker compose exec kafka kafka-topics \
		--bootstrap-server kafka:29092 \
		--list

ingest-base:
	docker compose exec airflow-webserver python /opt/airflow/ingestion/load_baf_to_duckdb.py \
		--source-file /opt/airflow/data/raw/Base.csv \
		--database /opt/airflow/warehouse/fraud.duckdb \
		--table raw.raw_baf_applications \
		--variant-name base \
		--replace

validate-base:
	docker compose exec airflow-webserver bash -c "cd /opt/airflow && python -m ingestion.validate_ingestion \
		--database /opt/airflow/warehouse/fraud.duckdb \
		--table raw.raw_baf_applications \
		--expected-rows 1000000 \
		--expected-min-month 0 \
		--expected-max-month 7"

test:
	docker compose run --rm --no-deps \
		-e CONNECTION_CHECK_MAX_COUNT=0 \
		-e PYTHONPATH=/opt/airflow \
		airflow-webserver bash -c \
		"cd /opt/airflow && pytest -q /opt/airflow/project-tests/test_ingestion.py"
