# Analytial Data Modeling (With On-Chain Data)

## Bitcoin Plus Plus Demo 2025

# Source Data
- To reduce file sizes, all source data has been limited to calendar year 2024, plus one month prior (Dec 2023) and one month after (Jan 2025), in case any look back or forward is needed.
- Bitcoin block data was sourced from a Bitcoin Core node (via Mempool Space's API): [https://mempool.space/docs/api/rest#get-blocks-bulk](https://mempool.space/docs/api/rest#get-blocks-bulk). **NOTE**: The bulk blocks endpoint is not enabled on the public site, it can be enabled on a self-hosted instance through the API config.
- Mining pool data was sourced from Mempool Space's open source repository of mining pools: [https://github.com/mempool/mining-pools/blob/master/pools-v2.json](https://github.com/mempool/mining-pools/blob/master/pools-v2.json)
- Bitcoin spot price data was sourced from Coinmetrics community (free tier) API: [https://docs.coinmetrics.io/api/v4/](https://docs.coinmetrics.io/api/v4/).
- Pool hashrate data was obtained from mining pools who provide this data freely, without needing API keys.

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

**NOTE**: The sample data provided was already pre-cleaned and validated. One notable callout with the Bitcoin block data is to build in checks for stale blocks (using `blockheight`, `block_hash` and `prev_block_hash`), left out here for simplicity.

## 01 Dimension Models

### 01:01 DIM Date
The date dimension model is part of the initial migrations and will be built automatically. This model contains one row per calendar date, with many helpful columns that can be used for analysis (e.g. `day_of_week`, `month_start_date`). This is a unique model where the primary surrogate key (`date_id`), a stringified date formatted as 'yyyymmdd'.

#### Data Granularity
- `date_id`

### 01:02 DIM Pool
Create a pool dimension model with a formatted display name and url, as well as categorical flags which are helpful for analaysis, such as `is_antpool_friend`.

#### Data Granularity
- `pool_id`

### 01:03 DIM Block
Create a Bitcoin block dimension model to hold any categorical data pertaining to each block, such as `block_hash`, `prev_block_hash`.

#### Data Granularity
- `block_id`

---

## 02 Fact Models

### 02:01 FACT Block
Create a Bitcoin block fact model with the `blockheight`, `timestamp`, and numerical data such as `block_size`, `reward_subsidy`. **NOTE**: if lower granularity than block-level data is never needed (IE no transaction-level data), it is possible to include all attributes from the Bitcoin block dimension model directly in this fact.  This practice is commonly referred to as a 'degenerate dimension'.

#### Data Granularity
- `block_id`

#### Foreign Key Relationships
- `dim.block.block_id`
- `dim.date.date_id`
- `dim.pool.pool_id`

### 02:02 FACT Network Stats 1d
Create a daily network stats fact model including `block_count`, `difficulty_weighted_avg` (a blended difficulty to account for adjustments), and `est_hashrate` (the estimated total network hashrate).

#### Data Granularity
- `date_id`

#### Foreign Key Relationships
- `dim.date.date_id`

### 02:03 FACT Pool Stats 1d
Create a daily pool stats fact model including `block_count`, `reported_hashrate` (for those pools who provide it).

#### Data Granularity
- `date_id`
- `pool_id`

#### Foreign Key Relationships
- `dim.date.date_id`
- `dim.pool.pool_id`

### 02:04 FACT Price 1d
Create a daily Bitcoin price fact model including `price_open`, `price_close`, and deriving `price_change`, `price_spread`, etc. **NOTE**: For simplicity, this only contains BTC-USD price data. If other coins or tickers are involved, a coin dimension table should also be built.

#### Data Granularity
- `date_id`

#### Foreign Key Relationships
- `dim.date.date_id`

---

## 03 OBT Models (Operational BI Table aka One Big Table)

### 03:01 OBT Block
Create a Bitcoin block OBT model which combines attributes from the Bitcoin block fact model, and the Bitcoin block, date, and pool dimension tables.

#### Data Granularity
- `block_id`

### 03:02 OBT Network Stats 1d
Create a daily network stats OBT model combines attributes from the daily network stats and price fact models, and the date dimension model. By joining the network stats and price fact models, USD amounts can be derived (including hashprice).

#### Data Granularity
- `date_id`

### 03:03 OBT Pool Stats 1d
Create a daily pool stats OBT model combines attributes from the pool stats, network stats, and price fact models, and the date and pool dimension models. By joining pool and network stats models, percent of network totals, expected block count, and mining luck (for those who provide reported hashrate) can be derived.

#### Data Granularity
- `date_id`
- `pool_id`

---

## Next Steps
### Additional Model Ideas (without existing sample data)
- An OBT model which contains one row per difficulty or subsidy epoch, with aggregate metrics pertaining to each epoch.
- An OBT model which contains one row per mining pool, and aggregates all time metrics pertaining to each pool.
- An OBT model which contains more sophisticated statistical modeling around mining 'luck' (aka variability).

### Additional Data Source Ideas
- Bitcoin transaction or address balance data (this is where the Bitcoin block dimension table is needed).
- Power consumption or price data from ERCOT or other power utilities.
