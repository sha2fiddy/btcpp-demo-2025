# Analytial Data Modeling (With On-Chain Data)

## Bitcoin Plus Plus Demo 2025

# Source Data
- To reduce file sizes, all source data has been limited to calendar year 2024, plus one month prior (Dec 2023) and one month after (Jan 2025), in case any look back or forward is needed.
- Bitcoin block data was sourced from a Bitcoin Core node (via Mempool Space's API): [https://mempool.space/docs/api/rest#get-blocks-bulk](https://mempool.space/docs/api/rest#get-blocks-bulk). **NOTE**: The bulk blocks endpoint is not enabled on the public site, it can be enabled on a self-hosted instance through the API config.
- Mining pool data was sourced from Mempool Space's open source repository of mining pools: [https://github.com/mempool/mining-pools/blob/master/pools-v2.json](https://github.com/mempool/mining-pools/blob/master/pools-v2.json)
- Bitcoin spot price data was sourced from Coinmetrics community (free tier) API: [https://docs.coinmetrics.io/api/v4/](https://docs.coinmetrics.io/api/v4/).

# Setup Instricutions

## 🚀 How to start Postgres and pgAdmin
```
docker compose up -d --build
docker compose up -d
```

## 🛑 How to stop and delete volumes
```
docker compose down -v
```

## 🛑 How to stop without deleting volumes
```
docker compose down
```

## 🌐 How to open pgAdmin
1. Open your browser and go to:  
[http://localhost:5050/browser/](http://localhost:5050/browser/)
2. Sign in and create server connection using the credentials in the .env file

## 📁 How to load new seed data with CSV files
1. Add your CSV file to the `./data` folder
2. Create a new `.sql` file inside `./migrations/` with a `COPY` command like:

   ```sql
   COPY your_table(<columns>) FROM '/data/your_file.csv' DELIMITER ',' CSV HEADER;
   ```

3. Mount this SQL file in `docker-compose.yml` under the `postgres` service:
   ```yaml
   - ./migrations/5-load-yourfile.sql:/docker-entrypoint-initdb.d/5-load-yourfile.sql
   ```

# Workshop Guide
For the purpose of the workshop, we will be creating models by running SQL directly in pgAdmin. In a production environment, it is highly recommended to create models through migration files with idempotency, or to use a data modeling framework such as dbt: [https://www.getdbt.com/](https://www.getdbt.com/).

Example SQL is given in the /models/ directory as a starting point. In the workshape we will materialize DIM and FACT models as tables, and OBT models as views. In a production environment, it is also recommended to add unique key constraints, column indexes, and other common database optimizations.

## DIM Date
The date dimension model is part of the initial migrations and will be built automatically. This model contains one row per calendar date, with many helpful columns that can be used for analysis (e.g. `day_of_week`, `month_start_date`). The primary surrogate key of the date dimension is `date_id`, a stringified date formatted as 'yyyymmdd'.

### Data Granularity
- `date_id`

## DIM Pool
Create a pool dimension model with a formatted display name and url, as well as flags which are helpful for analaysis (e.g. if the pool is part of 'Antpool & Friends'). The primary surrogate key of the pool dimension is `pool_id` which is an MD5 hash of the natural key, `pool_key`.

### Data Granularity
- `pool_id`

---

## FACT Block
Create a Bitcoin block fact model with the `blockheight`, `timestamp`, and numerical data such as `block_size`, `reward_subsidy`. **NOTE**: if you are dealing with transaction-level data, it may make sense to also have a Bitcoin block dimension model to contain the categorical data, and keep the numeric data in the fact model. Here, we are just creating a fact, and including categorical attributes such as `block_hash` directly (this practice is commonly referred to as a 'degenerate dimension').

### Data Granularity
- `block_hash`

### Foreign Key Relationships
- `date_id`
- `pool_id`

## FACT Network Stats 1d
Create a daily network stats model including `block_count`, `difficulty_weighted_avg` (a blended difficulty to account for adjustments), and `est_hashrate` (the estimated total network hashrate).

### Data Granularity
- `date_id`

### Foreign Key Relationships
- `date_id`

## FACT Pool Stats 1d
Create a daily pool stats model including `block_count`, `hashrate` (the reported pool hashrate), and `est_hashrate` (the estimated pool hashrate, if reported hashrate is unavailable).

### Data Granularity
- `date_id`
- `pool_id`

### Foreign Key Relationships
- `date_id`
- `pool_id`

---

## OBT Block
Create a Bitcoin block OBT model combines attributes from the Bitcoin block fact model, and the date and pool dimension tables.

### Data Granularity
- `block_hash`

## OBT Network Stats 1d
Create a daily network stats OBT model combines attributes from the network stats 1d fact model, and the date dimension model.

### Data Granularity
- `date_id`

## OBT Pool Stats 1d
Create a daily pool stats OBT model combines attributes from the network stats 1d and pool stats 1d fact models, and the date and pool dimension models.

### Data Granularity
- `date_id`
- `pool_id`

---

# Next Steps
Below are some ideas for additional models that could be built without requiring any additional source data:
- An OBT model which contains one row per difficulty or subsidy epoch, with aggregate metrics pertaining to each epoch.
- An OBT model which contains one row per mining pool, and aggregates all time metrics pertaining to each pool.
