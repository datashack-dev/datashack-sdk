.PHONY: build clean upload

clean:
	find . -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete
	rm -rf build dist datashack.egg-info sdk/py/datashack_sdk.egg-info

build: clean
	python setup.py bdist_wheel

upload:
	python -m twine upload dist/*