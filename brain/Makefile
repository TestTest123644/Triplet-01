PY := $(firstword $(wildcard .venv/Scripts/python.exe .venv/bin/python) python)

.PHONY: test

test:
	$(PY) -m pytest
