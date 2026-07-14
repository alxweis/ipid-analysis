#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_NAME = ipid-analysis
PYTHON_VERSION = 3.10
PYTHON_INTERPRETER = python

#################################################################################
# COMMANDS                                                                      #
#################################################################################


## Install Python dependencies
.PHONY: requirements
requirements:
	$(PYTHON_INTERPRETER) -m pip install -U pip
	$(PYTHON_INTERPRETER) -m pip install -r requirements.txt
	



## Delete all compiled Python files
.PHONY: clean
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete


## Lint using ruff (use `make format` to do formatting)
.PHONY: lint
lint:
	ruff format --check
	ruff check

## Format source code with ruff
.PHONY: format
format:
	ruff check --fix
	ruff format





## Set up Python interpreter environment
.PHONY: create_environment
create_environment:
	@bash -c "if [ ! -z `which virtualenvwrapper.sh` ]; then source `which virtualenvwrapper.sh`; mkvirtualenv $(PROJECT_NAME) --python=$(PYTHON_INTERPRETER); else mkvirtualenv.bat $(PROJECT_NAME) --python=$(PYTHON_INTERPRETER); fi"
	@echo ">>> New virtualenv created. Activate with:\nworkon $(PROJECT_NAME)"
	



#################################################################################
# PROJECT RULES                                                                 #
#################################################################################


## Make dataset
.PHONY: data
data: requirements
	$(PYTHON_INTERPRETER) ipid_analysis/dataset.py



## Run postprocessing (strategies + probing intervals) and plotting for a manifest
##   usage: make analyse data.json
.PHONY: analyse
analyse:
	$(PYTHON_INTERPRETER) ipid_analysis/postprocess.py $(filter-out analyse,$(MAKECMDGOALS))

# allow passing the manifest as a goal (`make analyse data.json`): make it a no-op target
%.json:
	@:

## Poll S3 for measurement handoff jobs
##   usage: make workflow-worker ARGS="--s3-prefix s3://bucket/prefix"
.PHONY: workflow-worker
workflow-worker:
	$(PYTHON_INTERPRETER) -m ipid_analysis.s3_workflow $(ARGS)

## Run the focused unit tests
.PHONY: test
test:
	$(PYTHON_INTERPRETER) -m unittest discover -s tests -v

#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys; \
lines = '\n'.join([line for line in sys.stdin]); \
matches = re.findall(r'\n## (.*)\n[\s\S]+?\n([a-zA-Z_-]+):', lines); \
print('Available rules:\n'); \
print('\n'.join(['{:25}{}'.format(*reversed(match)) for match in matches]))
endef
export PRINT_HELP_PYSCRIPT

help:
	@$(PYTHON_INTERPRETER) -c "${PRINT_HELP_PYSCRIPT}" < $(MAKEFILE_LIST)
