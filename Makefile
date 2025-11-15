.PHONY: up down build test

up:
	docker-compose up --build

down:
	docker-compose down -v

build:
	docker-compose build --no-cache

test:
	docker-compose run --rm pipeline pytest -q