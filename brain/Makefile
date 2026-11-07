PY := $(firstword $(wildcard .venv/Scripts/python.exe .venv/bin/python) python)
name ?= T02
profile ?= standalone
logs ?= logs
BOOK := T01 T02 T03 T04 T05 T06 T07 T08 T09 T10 T11 T12

.PHONY: test scenario standalone timeline book contracts

test:
	$(PY) -m pytest

scenario:
	$(PY) -m scenarios.runner --name $(name) --profile $(profile)

standalone:
	$(PY) -m city_brain --config config/standalone.yaml --scenario $(name)

timeline:
	$(PY) -m timeline --logs $(logs)

# Every scenario in the Test Book, each writing its own log and summary.
book: $(BOOK:%=book-%)

book-%:
	$(PY) -m scenarios.runner --name $* --profile standalone

contracts:
	$(PY) -m city_brain.contracts --out contracts
