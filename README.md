## Usage
1. Create a virtual environment (recommended) and install the necessary Python requirements
   ```
   python -m venv env && source env/bin/activate
   pip install -r requirements.txt
   ```
2. Set the required environment variables `DISCORD_TOKEN` and `DISCORD_SERVER_ID`
   ```
   export DISCORD_TOKEN=xyz
   export DISCORD_SERVER_ID=123
   ```
3. Install the DuckDB CLI. If on Mac: `brew install duckdb`
4. Run `python stats.py --backfill` to get all available data
5. Connect to your new DuckDB database and query it:
   ```
   duckdb
   .open server.db
   select count(*) from messages;
   .exit
   ```
6. To collect new messages since your initial backfill: `python stats.py`
   - This will use the maximum message `created_at` value for each channel to get new messages only
