.PHONY: publish test dev-env-mac dev-env-windows

all:
	@echo pg-data-etl makefile options include:
	@echo ------------------------------------
	@echo - dev-env-mac
	@echo - dev-env-windows
	@echo - test
	@echo - publish

dev-env-mac:
	poetry install

dev-env-windows:
	conda env create -f environment.yml

test:
	pytest .

publish:
	poetry publish --build
