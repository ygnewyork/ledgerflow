# LedgerFlow -- local development.
#
#   make setup    create a virtualenv and install the project
#   make demo     migrate, bootstrap, generate 90 days, drain the pipeline
#   make serve    API + dashboard on :8000
#   make test     the full suite
#
# Everything runs inside .venv, so there is no `python` vs `python3` and no
# `pip install` into the system interpreter.

VENV    := .venv
PY      := $(VENV)/bin/python
PIP     := $(VENV)/bin/pip
DB      ?= postgresql://$(USER)@localhost:5432/ledgerflow

export LEDGERFLOW_DATABASE_URL ?= $(DB)

.PHONY: help setup demo serve test lint clean db-create doctor

help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

$(VENV):
	python3 -m venv $(VENV)
	$(PIP) install --quiet --upgrade pip

setup: $(VENV)  ## create the virtualenv and install the project
	$(PIP) install --quiet -e ".[api,dev]"
	@echo "ready. next: make db-create && make demo"

db-create:  ## create the database (Homebrew postgres: no password needed)
	@createdb ledgerflow 2>/dev/null && echo "created database 'ledgerflow'" \
		|| echo "database 'ledgerflow' already exists (or createdb is not on PATH)"

demo: setup  ## migrate, bootstrap, generate 90 days of history, drain the pipeline
	$(PY) -m ledgerflow.cli migrate
	$(PY) -m ledgerflow.cli bootstrap | tee .bootstrap.json
	$(PY) -m ledgerflow.cli loadgen --days 90
	$(PY) -m ledgerflow.cli worker all
	@echo
	@echo "Now run:  make serve   then open http://localhost:8000/dashboard/"
	@echo "Test API key:"
	@$(PY) -c "import json;print('  '+json.load(open('.bootstrap.json'))['keys']['test'])"

serve: $(VENV)  ## run the API and dashboard on :8000
	$(PY) -m ledgerflow.cli serve

test: $(VENV)  ## run every test
	$(VENV)/bin/pytest -q
	$(PY) -m unittest discover -s tests

lint: $(VENV)
	$(VENV)/bin/ruff check src tests
	$(VENV)/bin/mypy

doctor:  ## check what is and is not installed
	@printf "python3    "; python3 --version 2>/dev/null \
		|| echo "MISSING -- run: xcode-select --install   (or: brew install python)"
	@printf "database   "
	@psql "$(LEDGERFLOW_DATABASE_URL)" -c 'select 1' >/dev/null 2>&1 \
		&& echo "reachable -- $(LEDGERFLOW_DATABASE_URL)" \
		|| { echo "UNREACHABLE -- $(LEDGERFLOW_DATABASE_URL)"; \
		     echo "             is postgres running?  brew services start postgresql@16"; \
		     echo "             does the database exist?  make db-create"; }
	@printf "redis      "
	@redis-cli $(if $(LEDGERFLOW_REDIS_URL),-u "$(LEDGERFLOW_REDIS_URL)",) ping 2>/dev/null \
		| grep -q PONG && echo "reachable" \
		|| echo "not reachable -- optional; the rate limiter falls back to Postgres"

clean:
	rm -rf $(VENV) data _checkpoints .bootstrap.json
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
