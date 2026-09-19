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

db-create:  ## create the role and database the app expects
	@createdb ledgerflow 2>/dev/null && echo "created database 'ledgerflow'" \
		|| echo "could not create 'ledgerflow' with $$(command -v createdb || echo createdb)"
	@# A Homebrew cluster makes your macOS username the superuser; an EDB
	@# install does not, so the role may simply not exist. Creating it is
	@# harmless when it already does.
	@psql -d postgres -tAc "select 1 from pg_roles where rolname='$(USER)'" 2>/dev/null \
		| grep -q 1 || psql -d postgres -c "create role \"$(USER)\" login superuser" 2>/dev/null \
		|| true
	@$(MAKE) --no-print-directory doctor

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

doctor:  ## check what is installed, what is running, and what shadows what
	@printf "python3      "; command -v python3 >/dev/null 2>&1 \
		&& echo "$$(python3 --version 2>&1)  [$$(command -v python3)]" \
		|| echo "MISSING -- run: xcode-select --install   (or: brew install python)"
	@printf "psql client  "; command -v psql >/dev/null 2>&1 \
		&& echo "$$(psql --version 2>&1 | awk '{print $$3}')  [$$(command -v psql)]" \
		|| echo "not on PATH"
	@# More than one Postgres on a Mac is the normal case, not an exotic one:
	@# the EDB installer puts its binaries in /Library/PostgreSQL/<v>/bin, which
	@# sits ahead of Homebrew on PATH and silently shadows brew's psql/createdb.
	@installs=$$(ls -d /Library/PostgreSQL/*/bin /opt/homebrew/opt/postgresql@*/bin \
		/usr/local/opt/postgresql@*/bin 2>/dev/null); \
	count=$$(echo "$$installs" | grep -c . || true); \
	if [ "$$count" -gt 1 ]; then \
		echo "             NOTE: $$count Postgres installations found --"; \
		echo "$$installs" | sed 's/^/                   /'; \
		echo "             the first on PATH wins; they may not be the one running."; \
	fi
	@printf "server       "
	@v=$$(psql "$(LEDGERFLOW_DATABASE_URL)" -tAc 'select version()' 2>/dev/null); \
	if [ -z "$$v" ]; then \
		v=$$(psql "postgresql://$(USER)@localhost:5432/postgres" -tAc 'select version()' 2>/dev/null); \
	fi; \
	if [ -z "$$v" ]; then \
		echo "no server answering on localhost:5432"; \
	else \
		echo "$$v" | cut -d, -f1; \
	fi
	@printf "database     "
	@psql "$(LEDGERFLOW_DATABASE_URL)" -c 'select 1' >/dev/null 2>&1 \
		&& echo "ready -- $(LEDGERFLOW_DATABASE_URL)" \
		|| { echo "unreachable -- $(LEDGERFLOW_DATABASE_URL)"; \
		     echo "             if the server line says no server is answering, start one:"; \
		     echo "               brew services start postgresql@16"; \
		     echo "             if a server IS up but the role or database is missing:"; \
		     echo "               make db-create"; \
		     echo "             if the running server wants a different user, set it:"; \
		     echo "               export LEDGERFLOW_DATABASE_URL=postgresql://USER:PASS@localhost:5432/ledgerflow"; }
	@printf "redis        "
	@redis-cli $(if $(LEDGERFLOW_REDIS_URL),-u "$(LEDGERFLOW_REDIS_URL)",) ping 2>/dev/null \
		| grep -q PONG && echo "ready" \
		|| echo "not running -- optional; the rate limiter falls back to Postgres"

clean:
	rm -rf $(VENV) data _checkpoints .bootstrap.json
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
