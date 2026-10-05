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
	$(PYTHON_INTERPRETER) ipid_analysis/postprocess.py $(filter-out analyse,$(MAKECMDGOALS)) $(ARGS)

# allow passing the manifest as a goal (`make analyse data.json`): make it a no-op target
%.json:
	@:

## Interactively inspect sampled raw sequences for one classified strategy
##   usage: make inspect-sequences ARGS="<target> --manifest data.json"
.PHONY: inspect-sequences
inspect-sequences:
	$(PYTHON_INTERPRETER) -m ipid_analysis.inspect_sequences $(ARGS)

## Analyze observed missing replies in persisted fixed-interval Mass sequences
##   usage: make analyse-missing-replies ARGS="data.json"
.PHONY: analyse-missing-replies
analyse-missing-replies:
	$(PYTHON_INTERPRETER) -m ipid_analysis.missing_reply_analysis $(ARGS)

## Diagnose RT-Base UNCLASSIFIED addresses later classified deterministically in Mass
##   usage: make analyse-deterministic-transitions ARGS="data.json"
.PHONY: analyse-deterministic-transitions
analyse-deterministic-transitions:
	$(PYTHON_INTERPRETER) -m ipid_analysis.deterministic_transition_analysis $(ARGS)

## Poll S3 for RT handoff and complete postprocessing jobs
##   usage: make workflow-worker ARGS="--s3-prefix s3://bucket/prefix"
.PHONY: workflow-worker
workflow-worker:
	$(PYTHON_INTERPRETER) -m ipid_analysis.s3_workflow $(ARGS)

## Run the focused unit tests
.PHONY: test
test:
	$(PYTHON_INTERPRETER) -m unittest discover -s tests -v

## Validate the classifier and plot synthetic strategy diagnostics
.PHONY: validate-classifier
validate-classifier:
	$(PYTHON_INTERPRETER) -m ipid_analysis.classifier_validation $(ARGS)
	$(PYTHON_INTERPRETER) -m ipid_analysis.plot_random_structure_score_cdf $(ARGS)

## Plot selected candidate RANDOM-score CDFs for synthetic Mass 4x25 sequences
.PHONY: plot-mass-random-score-cdf
plot-mass-random-score-cdf:
	$(PYTHON_INTERPRETER) -m ipid_analysis.plot_random_structure_score_cdf $(ARGS)

## Plot the adapted NIST SP 800-22 short-IPID baseline
.PHONY: plot-nist-short-sequence-baseline
plot-nist-short-sequence-baseline:
	$(PYTHON_INTERPRETER) -m ipid_analysis.plot_nist_short_sequence_baseline $(ARGS)

## Plot held-out current/NIST/candidate RANDOM-classifier diagnostics
.PHONY: plot-random-classifier-diagnostics
plot-random-classifier-diagnostics:
	$(PYTHON_INTERPRETER) -m ipid_analysis.plot_random_classifier_diagnostics $(ARGS)

## Evaluate all RANDOM-score metrics and their 63 non-empty combinations
.PHONY: evaluate-random-classifier
evaluate-random-classifier:
	$(PYTHON_INTERPRETER) -m ipid_analysis.random_classifier_evaluation $(ARGS)

## Accuracy-first RANDOM metric evaluation with independent selection/held-out generators
.PHONY: evaluate-random-classifier-v2
evaluate-random-classifier-v2:
	$(PYTHON_INTERPRETER) -m ipid_analysis.random_classifier_evaluation_v2 $(ARGS)

## Compare increment-uniformity bin rules without changing production
.PHONY: evaluate-increment-bin-rules
evaluate-increment-bin-rules:
	$(PYTHON_INTERPRETER) -m ipid_analysis.increment_bin_rule_evaluation $(ARGS)

## Screen/confirm RANDOM-classifier view aggregation without changing production
.PHONY: evaluate-random-classifier-views
evaluate-random-classifier-views:
	$(PYTHON_INTERPRETER) -m ipid_analysis.random_classifier_view_evaluation $(ARGS)

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
