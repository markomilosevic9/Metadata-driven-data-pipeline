.PHONY: urls build up down restart logs clean status

urls:
	@echo URLs
	@echo - Airflow UI:         http://localhost:8080 (admin/admin)
	@echo - Spark Master UI:    http://localhost:8081
	@echo - Spark History:      http://localhost:18080
	@echo - MinIO Console:      http://localhost:9001 (minioadmin/minioadmin)

build:
	@echo Building Docker images
	docker-compose build

up:
	@echo Starting all services
	docker-compose up -d
	@echo Services are starting up

down:
	@echo Stopping all services
	docker-compose down

restart: 
	@echo Restarting
	down up

logs:
	@echo Checking logs
	docker-compose logs -f

status:
	@echo Checking container status
	docker-compose ps

clean:
	@echo Cleaning up
	docker-compose down -v
	@echo Clean-up complete
