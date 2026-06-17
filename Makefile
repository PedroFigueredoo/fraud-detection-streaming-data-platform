up:
	docker compose up -d --build

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
