.PHONY: publish test dev-env-mac dev-env-windows format docs notebook

all:
	@echo pg-data-etl makefile options include:
	@echo ------------------------------------
	@echo - dev-env-mac
	@echo - dev-env-windows
	@echo - test
	@echo - format
	@echo - docs
	@echo - notebook
	@echo - publish

dev-env-mac:
	poetry install

dev-env-windows:
	conda env create -f environment.yml

test:
	pytest .

publish:
	poetry publish --build

format:
	black -l 100 .

docs:
	mkdocs serve

notebook:
	jupyter lab