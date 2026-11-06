PY := $(firstword $(wildcard .venv/Scripts/python.exe .venv/bin/python) python)
name ?= T02
profile ?= standalone

.PHONY: test scenario standalone

test:
	$(PY) -m pytest

scenario:
	$(PY) -m scenarios.runner --name $(name) --profile $(profile)

standalone:
	$(PY) -m city_brain --config config/standalone.yaml --scenario $(name)
