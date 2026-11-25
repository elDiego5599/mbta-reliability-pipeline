.PHONY: status

DATA_DIR ?= data

status:
	uv run mbta-status --data-dir $(DATA_DIR)
