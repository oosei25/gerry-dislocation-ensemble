# gerry-dislocation-ensemble

Voter-level (precinct-weighted) comparisons of enacted district context vs ensemble counterfactuals using Atlas runs, with non–county-preserving ensembles as the baseline and county-preserving ensembles as a constraints check.

## Current status

Phase 1 ingestion and validation is in place.

### What is working now:

* graph loading for `NC_pct21.json`
* streaming Atlas `.jsonl.gz` reading
* extraction of unit-to-district assignments from plan records
* validation tests for graph/Atlas unit alignment
* a Phase 1 validation notebook documenting the current schema and assumptions

### Current finding:

* the non-county baseline atlas aligns cleanly with `NC_pct21.json`
* the county-preserving atlas appears to use a different unit representation and will require the matching graph/geography file or a crosswalk before direct precinct-level comparison

## Setup

```bash
conda env create -f environment.yml
conda activate gerry-dislocation
make install
```

## Fetch inputs + atlases

```bash
make fetch-atlases
make verify-atlases
```

> Downloaded files are saved under data/raw/ and are gitignored.

## Run checks

```bash
make test
make lint
```

## Notes

* `configs/runs.yaml` is the source of truth for what data is used.
* Atlas files are `.jsonl.gz` and can be streamed without manual decompression.
* The current phase 1 validation workflow is documented in `notebooks/00_phase1_validation.ipynb`. 
