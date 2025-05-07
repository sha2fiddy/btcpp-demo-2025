# Analytial Data Modeling (With On-Chain Data)

## btc++ Austin 2025

# Repo Contents
- Docker compose containing:
   - Postgres
   - pgAdmin (a GUI toolset for Postgres)
   - Automatically run initial SQL migrations to load sample data
- A /data/ dir with sample data csv's
- A /migrations/ dir with scripts to create schemas (src, dim, fact, obt), and load csv's to src tables
- A /models/ dir with sample data models (dim, fact, obt)
- Slides from btc++ presentation
- Additional resources and ideas for further development

# Workshop Goals
- Create star schemas:
   - 1. Dimensions: `date`, `pool`, `block`
   - 2. Facts: `block`, `network_stats_1d`, `pool_stats_1d`, `price_1d`
- Create denormalized datasets:
   - 3. OBTs: `block`, `network_stats_1d`, `pool_stats_1d`
- From raw Bitcoin block data, derive metrics including:
   - `estimated_hashrate` (network-level)
   - `hashvalue`
   - `hashprice` (using daily BTC:USD price)
   - `mining_luck` (using pool reported hashrates)

# Source Data
- To reduce file sizes, all source data has been limited to calendar year 2024, plus one month prior (Dec 2023) and one month after (Jan 2025), in case any look back or forward is needed.
- Bitcoin block data was sourced from a Bitcoin Core node (via Mempool Space's API): [https://mempool.space/docs/api/rest#get-blocks-bulk](https://mempool.space/docs/api/rest#get-blocks-bulk). The attribute list returned from this endpoint has been reduced to keep the sample data files small. **NOTE**: The bulk blocks endpoint is not enabled on the public Mempool Space site, but it can be enabled on a self-hosted instance through the API config.
- Mining pool data was sourced from Mempool Space's open source repository of pools: [https://github.com/mempool/mining-pools/blob/master/pools-v2.json](https://github.com/mempool/mining-pools/blob/master/pools-v2.json)
- Bitcoin spot price data was sourced from Coinmetrics community (free tier) API: [https://docs.coinmetrics.io/api/v4/](https://docs.coinmetrics.io/api/v4/).
- Pool hashrate data was obtained from several mining pools who provide this data freely, without needing accounts or API keys.

# Setup Instructions

#### 🚀 How to start Postgres and pgAdmin
- Initial build:
   ```
   docker compose up -d --build
   ```
- If already built:
   ```
   docker compose up -d
   ```

#### 🛑 How to stop Postgres and pgAdmin
- With deleting data volumes:
   ```
   docker compose down -v
   ```
-  Without deleting data volumes:
   ```
   docker compose down
   ```

#### 🌐 How to open pgAdmin
1. Open your browser and go to: [http://localhost:5050/browser/](http://localhost:5050/browser/)
2. Sign in and create server connection using the credentials in the .env file

#### 📁 How to load additional seed data with csv files
1. Add the csv file to the `./data` folder
2. Create a new `.sql` file inside `./migrations/` with a `copy` command like:
   ```sql
   copy <table>(<columns>) from '/data/<filename>.csv' delimiter ',' csv header;
   ```
3. Mount this SQL file in `docker-compose.yml` under the `postgres` service:
   ```yaml
   ./migrations/<sequence>-<name>.sql:/docker-entrypoint-initdb.d/<sequence>-<name>.sql
   ```

# Workshop Guide
For the purpose of the workshop, we will be creating models by running SQL directly in pgAdmin. In a production environment, it is highly recommended to materialize models through migration files with idempotency, or to use a data modeling framework such as dbt: [https://www.getdbt.com/](https://www.getdbt.com/).

Example SQL is given in the /models/ directory as a starting point. In the workshape we will materialize DIM and FACT models as tables, and OBT models as views. In a production environment, it is also recommended to add unique key constraints, column indexes, and other common database optimizations.

**NOTE**: The sample data provided was already pre-cleaned and validated. One notable callout with the Bitcoin block data is to build in checks for stale blocks (using `blockheight`, `block_hash` and `prev_block_hash`), left out here for simplicity.

## 1 Dimension Models

### 1.1 DIM Date
The date dimension model is part of the initial migrations and will be built automatically. This model contains one row per calendar date, with many helpful columns that can be used for analysis (e.g. `day_of_week`, `month_start_date`). This is a unique model where the primary surrogate key (`date_id`) is a stringified date, formatted as 'yyyymmdd'.

#### Data Granularity
- `date_id`

### 1.2 DIM Pool
Create a pool dimension model with a formatted display name and url, as well as categorical flags which are helpful for analaysis, such as `is_antpool_friend`.

#### Data Granularity
- `pool_id`

### 1.3 DIM Block
Create a Bitcoin block dimension model to hold any categorical data pertaining to each block, such as `block_hash`, `is_subsidy_halving`, `is_difficulty_adjustment`.

#### Data Granularity
- `block_id`

---

## 2 Fact Models

### 2.1 FACT Block
Create a Bitcoin block fact model with the `blockheight`, `timestamp`, and numerical data such as `block_size`, `reward_subsidy`. **NOTE**: if lower than block-level data granularity is never needed (IE no transaction-level data), it is possible to include all attributes from the Bitcoin block dimension model directly in this fact.  This practice is commonly referred to as 'degenerate dimension' attributes.

#### Data Granularity
- `block_id`

#### Foreign Key Relationships
- `dim.block.block_id`
- `dim.date.date_id`
- `dim.pool.pool_id`

### 2.2 FACT Network Stats 1d
Create a daily network stats fact model including `block_count`, `difficulty_weighted_avg` (a blended difficulty to account for adjustments), and `est_hashrate` (the estimated total network hashrate).

#### Data Granularity
- `date_id`

#### Foreign Key Relationships
- `dim.date.date_id`

### 2.3 FACT Pool Stats 1d
Create a daily pool stats fact model including `block_count`, `reported_hashrate` (for those pools who provide it).

#### Data Granularity
- `date_id`
- `pool_id`

#### Foreign Key Relationships
- `dim.date.date_id`
- `dim.pool.pool_id`

### 2.4 FACT Price 1d
Create a daily Bitcoin price fact model including `price_open`, `price_close`, and deriving `price_change`, `price_spread`, etc. **NOTE**: For simplicity, this only contains BTC-USD price data. If other coins or tickers are involved, a coin dimension table should also be built.

#### Data Granularity
- `date_id`

#### Foreign Key Relationships
- `dim.date.date_id`

---

## 3 Operational BI Table (OBT) Models

### 3.1 OBT Block
Create a Bitcoin block OBT model which combines attributes from the Bitcoin block fact model, and the Bitcoin block, date, and pool dimension tables.

#### Data Granularity
- `block_id`

### 3.2 OBT Network Stats 1d
Create a daily network stats OBT model combines attributes from the daily network stats and price fact models, and the date dimension model. By joining the network stats and price fact models, USD amounts can be derived (including hashprice).

#### Data Granularity
- `date_id`

### 3.3 OBT Pool Stats 1d
Create a daily pool stats OBT model which combines attributes from the pool stats, network stats, and price fact models, and the date and pool dimension models. By joining pool and network stats models, it is possible to derive percent of network totals, expected block count, and mining luck (for those who provide reported hashrate).

#### Data Granularity
- `date_id`
- `pool_id`

---

## 4 Next Steps
### 4.1 Additional Model Ideas (without existing sample data)
- An OBT model which contains one row per difficulty or subsidy epoch, with aggregate metrics pertaining to each epoch.
- An OBT model which contains one row per mining pool, which aggregates all time metrics pertaining to each pool.
- An OBT model which contains more sophisticated statistical modeling around mining 'luck' (aka variability).

### 4.2 Additional Data Source Ideas
- Bitcoin transactions or address balances, for financial accounting or on-chain forensics (this is when the Bitcoin block dimension table is required).
- Stratum job templates or Bitcoin node logs, to analyze mining pool centralization or block relay efficiency.
- Power consumption or price history, to analyze hashrate correlation or mining hardware efficiency.

# Resources
#### Data Modeling Fundamdentals
- What is Data Modeling?: [https://aws.amazon.com/what-is/data-modeling/](https://aws.amazon.com/what-is/data-modeling/)
- Types of Data Models: [https://en.wikipedia.org/wiki/Database_model](https://en.wikipedia.org/wiki/Database_model)

#### Columnar Databases
- About Columnar Databases: [https://databasetown.com/columnar-databases/](https://databasetown.com/columnar-databases/)

#### Data Build Tool (dbt)
- dbt: [https://docs.getdbt.com/](https://docs.getdbt.com/)
- What is dbt?: [https://docs.getdbt.com/docs/introduction](https://docs.getdbt.com/docs/introduction)
- dbt Resources: [https://github.com/Hiflylabs/awesome-dbt](https://github.com/Hiflylabs/awesome-dbt)

#### dbt Alernatives
**NOTE**: No experience with or recommendation of these tools.
- SQLMesh: [https://sqlmesh.com/](https://sqlmesh.com/)
- Coalesce: [https://coalesce.io/](https://coalesce.io/)
