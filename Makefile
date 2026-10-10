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

## Download/import and cache one CAIDA IPv4 ITDK release
##   usage: make prepare-itdk ARGS="--release 2025-08 --topology midar-iff-snmp-tnt"
.PHONY: prepare-itdk
prepare-itdk:
	$(PYTHON_INTERPRETER) -m ipid_analysis.caida_itdk $(ARGS)

## Download/import and cache RIPE Atlas traceroute roles for one campaign
##   usage: make prepare-ripe-atlas ARGS="--campaign-start 2026-09-21T02:09:12Z"
.PHONY: prepare-ripe-atlas
prepare-ripe-atlas:
	$(PYTHON_INTERPRETER) -m ipid_analysis.ripe_atlas $(ARGS)

## Rebuild only CAIDA/RIPE role analyses from existing strategy/OS artifacts
##   usage: make analyse-network-roles MANIFEST=data/analysis-jobs/.../manifest.json
.PHONY: analyse-network-roles
analyse-network-roles:
	$(PYTHON_INTERPRETER) -m ipid_analysis.network_roles $(MANIFEST) $(ARGS)

## Benchmark matched-only network-role joins on a synthetic population
##   usage: make benchmark-network-roles ARGS="--rows 300000000 --match-stride 1000"
.PHONY: benchmark-network-roles
benchmark-network-roles:
	$(PYTHON_INTERPRETER) benchmarks/network_role_join.py $(ARGS)

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

## Freeze Mass UNCLASSIFIED plus RANDOM controls for repeated measurements
##   usage: make prepare-random-reproducibility ARGS="prepare data.json --target icmp.ipid.no-connection.fixed-interval.mass"
.PHONY: prepare-random-reproducibility
prepare-random-reproducibility:
	$(PYTHON_INTERPRETER) -m ipid_analysis.random_reproducibility_analysis $(ARGS)

## Evaluate repeated raw Mass runs without overwriting strategies.pq
##   usage: make analyse-random-reproducibility ARGS="evaluate data.json --repeat-id id-1 --repeat-id id-2"
.PHONY: analyse-random-reproducibility
analyse-random-reproducibility:
	$(PYTHON_INTERPRETER) -m ipid_analysis.random_reproducibility_analysis $(ARGS)

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

## Build same-strategy ICMP/TCP/UDP target intersections
.PHONY: build-interprotocol-targets
build-interprotocol-targets:
	$(PYTHON_INTERPRETER) -m ipid_analysis.interprotocol build-targets $(ARGS)

## Classify an inter-protocol measurement and render deployment plots
.PHONY: analyse-interprotocol
analyse-interprotocol:
	$(PYTHON_INTERPRETER) -m ipid_analysis.interprotocol classify $(ARGS)

## Classify every measurement in an inter-protocol campaign run
.PHONY: analyse-interprotocol-campaign
analyse-interprotocol-campaign:
	$(PYTHON_INTERPRETER) -m ipid_analysis.interprotocol analyse-campaign $(ARGS)

## Validate the inter-protocol classifier against synthetic ground truth
.PHONY: validate-interprotocol
validate-interprotocol:
	$(PYTHON_INTERPRETER) -m ipid_analysis.interprotocol validate $(ARGS)

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
