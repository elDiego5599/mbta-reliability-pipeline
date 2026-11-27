.PHONY: status dbt-build dbt-fixture

DATA_DIR ?= data
MBTA_DATA_DIR ?= $(DATA_DIR)

status:
	uv run mbta-status --data-dir $(DATA_DIR)

dbt-build:
	cd dbt && DBT_PROFILES_DIR=. MBTA_DATA_DIR=$(abspath $(MBTA_DATA_DIR)) uv run dbt build

dbt-fixture:
	@set -eu; fixture_dir="$$(mktemp -d)"; trap 'rm -rf "$$fixture_dir"' EXIT; \
	uv run python tests/dbt_fixture.py "$$fixture_dir"; \
	cd dbt && DBT_PROFILES_DIR=. MBTA_DATA_DIR="$$fixture_dir" \
	MBTA_WAREHOUSE="$$fixture_dir/wh.duckdb" \
	uv run dbt build --vars '{fixture_build: true}'
