PYTHON := python
PIP := $(PYTHON) -m pip

.PHONY: install infra-up infra-down pipeline pipeline-% test format lint

install:
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

infra-up:
	docker compose up -d

infra-down:
	docker compose down -v

pipeline:
	$(PYTHON) -m pipeline.runner run --config config/pipeline.yaml

pipeline-%:
	$(PYTHON) -m pipeline.runner run --config config/$*.yaml

test:
	$(PYTHON) -m pytest -q