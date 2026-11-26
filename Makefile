.PHONY: status dbt-build

DATA_DIR ?= data
MBTA_DATA_DIR ?= $(DATA_DIR)

status:
	uv run mbta-status --data-dir $(DATA_DIR)

dbt-build:
	cd dbt && DBT_PROFILES_DIR=. MBTA_DATA_DIR=$(abspath $(MBTA_DATA_DIR)) uv run dbt build
