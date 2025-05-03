# Postgres + PGAdmin Setup

### 🚀 How to start
```
docker compose up -d --build
docker compose up -d
```

### 🛑 How to stop and delete volumes
```
docker compose down -v
```

### 🛑 How to stop without deleting volumes
```
docker compose down
```

### 🌐 How to open pgAdmin
Open your browser and go to:  
[http://localhost:5050/browser/](http://localhost:5050/browser/)

### How to use pgAdmin
Sign in and create server connection using the credentials in the .env file

### 📁 How to load new CSV files
1. Add your CSV file to the `./data` folder (or use a different mapped folder if set up).
2. Create a new `.sql` file inside `./migrations/` with a `COPY` command like:

   ```sql
   COPY your_table(<columns>) FROM '/data/your_file.csv' DELIMITER ',' CSV HEADER;
   ```

3. Mount this SQL file in `docker-compose.yml` under the `postgres` service:
   ```yaml
   - ./migrations/5-load-yourfile.sql:/docker-entrypoint-initdb.d/5-load-yourfile.sql
   ```
