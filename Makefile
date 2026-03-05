.PHONY: fetch-atlases verify-atlases install test lint

install:
	pip install -r requirements.txt

fetch-atlases:
	python scripts/fetch_atlases.py --config configs/runs.yaml

verify-atlases:
	python scripts/fetch_atlases.py --config configs/runs.yaml --verify-only

test:
	pytest -q

lint:
	ruff check .