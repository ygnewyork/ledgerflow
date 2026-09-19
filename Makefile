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

brew-pg-5433:  ## move Homebrew's postgres to 5433, leaving another install on 5432
	@conf=$$(ls -d /opt/homebrew/var/postgresql@16 /usr/local/var/postgresql@16 \
		2>/dev/null | head -1)/postgresql.conf; \
	if [ ! -f "$$conf" ]; then echo "no Homebrew postgresql@16 data directory found"; exit 1; fi; \
	brew services stop postgresql@16 >/dev/null 2>&1 || true; \
	grep -q "^port = 5433" "$$conf" || printf "\nport = 5433\n" >> "$$conf"; \
	brew services start postgresql@16; \
	echo; \
	echo "Homebrew postgres now on 5433. Add these to your shell:"; \
	echo "  export PATH=\"$$(brew --prefix postgresql@16)/bin:\$$PATH\""; \
	echo "  export LEDGERFLOW_DATABASE_URL=\"postgresql://$(USER)@localhost:5433/ledgerflow\""

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
	@# -w so psql never prompts: an interactive password prompt inside a
	@# diagnostic hangs the terminal and makes "unreachable" indistinguishable
	@# from "reachable but needs credentials" -- which are opposite problems.
	@out=$$(PGCONNECT_TIMEOUT=3 psql -w "$(LEDGERFLOW_DATABASE_URL)" \
		-tAc 'select version()' 2>&1); \
	case "$$out" in \
		PostgreSQL*) echo "$$out" | cut -d, -f1 ;; \
		*"Connection refused"*|*"could not translate"*|*"timeout expired"*) \
			echo "nothing listening -- $(LEDGERFLOW_DATABASE_URL)"; \
			echo "             start one:  brew services start postgresql@16" ;; \
		*) \
			probe=$$(PGCONNECT_TIMEOUT=3 psql -w \
				"postgresql://postgres@localhost:5432/postgres" \
				-tAc 'select version()' 2>&1); \
			case "$$probe" in \
				PostgreSQL*) echo "$$probe" | cut -d, -f1 ;; \
				*) echo "a server IS listening but refused these credentials" ;; \
			esac; \
			echo "             $$(echo "$$out" | head -1)" ;; \
	esac
	@printf "database     "
	@out=$$(PGCONNECT_TIMEOUT=3 psql -w "$(LEDGERFLOW_DATABASE_URL)" -c 'select 1' 2>&1); \
	case "$$out" in \
		*"1 row"*|*"(1 row)"*) echo "ready -- $(LEDGERFLOW_DATABASE_URL)" ;; \
		*role*"does not exist"*) \
			echo "no such role -- $(LEDGERFLOW_DATABASE_URL)"; \
			echo "             the server is up but has no login role for this user."; \
			echo "             A Homebrew cluster makes your macOS username a superuser;"; \
			echo "             an EDB install only creates 'postgres'. Either:"; \
			echo "               export LEDGERFLOW_DATABASE_URL=postgresql://postgres:PASS@localhost:5432/ledgerflow"; \
			echo "             or give Homebrew's its own port:  make brew-pg-5433" ;; \
		*database*"does not exist"*) \
			echo "missing -- $(LEDGERFLOW_DATABASE_URL)"; \
			echo "             the server is up; create the database:  make db-create" ;; \
		*"Connection refused"*|*"timeout expired"*) \
			echo "no server -- $(LEDGERFLOW_DATABASE_URL)"; \
			echo "             brew services start postgresql@16" ;; \
		*"password"*|*"authentication"*) \
			echo "wrong credentials -- $(LEDGERFLOW_DATABASE_URL)"; \
			echo "             the server on this port wants a different user or a password."; \
			echo "             Two Postgres installs? The other one may own 5432."; \
			echo "             Either point at it:"; \
			echo "               export LEDGERFLOW_DATABASE_URL=postgresql://postgres:PASS@localhost:5432/ledgerflow"; \
			echo "             or move Homebrew's to its own port:  make brew-pg-5433" ;; \
		*) echo "unreachable -- $$(echo "$$out" | head -1)" ;; \
	esac
	@printf "redis        "
	@redis-cli $(if $(LEDGERFLOW_REDIS_URL),-u "$(LEDGERFLOW_REDIS_URL)",) ping 2>/dev/null \
		| grep -q PONG && echo "ready" \
		|| echo "not running -- optional; the rate limiter falls back to Postgres"

clean:
	rm -rf $(VENV) data _checkpoints .bootstrap.json
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
