# Chronicling America bulk download shortcuts.
#
# Usage:
#   make download              # run resumable pilot download
#   make dry-run               # print plan without fetching
#   make download CA_ROOT=/path/to/ca
#
# Override any variable on the command line, e.g.:
#   make download DELAY=5.0 MAX_RPM=5

.PHONY: help download dry-run dirs

PYTHON            ?= python
CA_ROOT           ?= /rcp-scratch/students/impresso-CA-pilot/ca
CONFIG            ?= text_preparation/importers/chronicling_america/chronicling_america_pilot_titles.json

OUTPUT_DIR        ?= $(CA_ROOT)/raw
STATE_DIR         ?= $(CA_ROOT)/state

# Rate-limit defaults (see bulk.py and LOC guidance)
WORKERS           ?= 1
DELAY             ?= 3.0
MAX_RPM           ?= 8
ASSET_MAX_RPM     ?=
ASSET_DELAY       ?=
DIRECTORY_DELAY   ?= 6.0
BATCH_COOLDOWN    ?= 180
ENUMERATION_COOLDOWN ?= 120
METS_BURST_SIZE   ?= 15
METS_BURST_PAUSE  ?= 90

# Optional tier-separated asset limits (empty = same as crawl defaults)
ASSET_RPM_FLAG    = $(if $(ASSET_MAX_RPM),--asset-max-rpm $(ASSET_MAX_RPM),)
ASSET_DELAY_FLAG  = $(if $(ASSET_DELAY),--asset-delay $(ASSET_DELAY),)

FETCH             = $(PYTHON) -m text_preparation.importers.chronicling_america.fetch_data
FETCH_ARGS        = --config $(CONFIG) \
                    --output-dir $(OUTPUT_DIR) \
                    --state-dir $(STATE_DIR) \
                    --workers $(WORKERS) \
                    --delay $(DELAY) \
                    --max-rpm $(MAX_RPM) \
                    $(ASSET_RPM_FLAG) \
                    $(ASSET_DELAY_FLAG) \
                    --directory-delay $(DIRECTORY_DELAY) \
                    --batch-cooldown $(BATCH_COOLDOWN) \
                    --enumeration-cooldown $(ENUMERATION_COOLDOWN) \
                    --mets-burst-size $(METS_BURST_SIZE) \
                    --mets-burst-pause $(METS_BURST_PAUSE) \
                    --keep-tarballs

help:
	@echo "Chronicling America download targets:"
	@echo ""
	@echo "  make download     Run resumable bulk download (pilot titles)"
	@echo "  make dry-run      Print download plan without fetching"
	@echo "  make dirs         Create CA_ROOT/raw and CA_ROOT/state"
	@echo ""
	@echo "Variables (override on the command line):"
	@echo "  CA_ROOT=$(CA_ROOT)"
	@echo "  CONFIG=$(CONFIG)"
	@echo "  DELAY=$(DELAY)  MAX_RPM=$(MAX_RPM)  ASSET_MAX_RPM=$(ASSET_MAX_RPM)  DIRECTORY_DELAY=$(DIRECTORY_DELAY)"
	@echo "  BATCH_COOLDOWN=$(BATCH_COOLDOWN)  ENUMERATION_COOLDOWN=$(ENUMERATION_COOLDOWN)"
	@echo "  METS_BURST_SIZE=$(METS_BURST_SIZE)  METS_BURST_PAUSE=$(METS_BURST_PAUSE)  WORKERS=$(WORKERS)"
	@echo ""
	@echo "High-throughput trial (soak-test before production):"
	@echo "  make download MAX_RPM=8 ASSET_MAX_RPM=20 METS_BURST_SIZE=0 METS_BURST_PAUSE=0 BATCH_COOLDOWN=30 ENUMERATION_COOLDOWN=30"

dirs:
	mkdir -p "$(OUTPUT_DIR)" "$(STATE_DIR)"

download: dirs
	$(FETCH) $(FETCH_ARGS)

dry-run: dirs
	$(FETCH) $(FETCH_ARGS) --dry-run
