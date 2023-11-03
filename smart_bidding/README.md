# smart-bidding

## Usage

```
python -m smart_bidding.etl.run_etl --help
```

Example:

```
python -m smart_bidding.etl.run_etl \
    --start_date "2023-10-25 00:00:00" \
    --end_date "2023-10-31 23:59:59" \
    --down_sampling_percentage 0.01 \
    --max_workers 8 \
    --storage_type remote
```
