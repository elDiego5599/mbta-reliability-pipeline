.PHONY: status dbt-build

DATA_DIR ?= data

status:
	uv run mbta-status --data-dir $(DATA_DIR)

dbt-build:
	cd dbt && DBT_PROFILES_DIR=. uv run dbt build
