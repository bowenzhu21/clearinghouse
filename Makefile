PYTHON ?= python3

.PHONY: demo test bench serve worker
demo:
	$(PYTHON) -m clearinghouse demo
test:
	$(PYTHON) -m unittest discover -v
bench:
	$(PYTHON) -m clearinghouse bench --operations 1000 --output demo-output/benchmark.json
serve:
	$(PYTHON) -m clearinghouse serve
worker:
	$(PYTHON) -m clearinghouse worker
