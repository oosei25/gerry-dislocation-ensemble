# gerry-dislocation-ensemble

Voter-level (precinct-weighted) comparisons of enacted district context vs ensemble counterfactuals using Atlas runs, with non–county-preserving ensembles as the baseline and county-preserving ensembles as a constraints check.

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