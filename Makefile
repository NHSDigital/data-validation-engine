activate = poetry run

# dev
install:
	poetry lock
	poetry install --with dev

# dist
wheel:
	poetry build -f wheel
	poetry build -f sdist

dist: wheel

# testing
behave:
	${activate} behave

pytest:
	${activate} pytest tests/

all-tests: pytest behave

coverage:
	$(activate) coverage run
	$(activate) coverage combine
	$(activate) coverage report
	$(activate) coverage xml

# lint
pylint:
	${activate} pylint src/

mypy:
	${activate} mypy src/

lint: mypy pylint

# pre-commit
pre-commit-all:
	${activate} pre-commit run --all-files
