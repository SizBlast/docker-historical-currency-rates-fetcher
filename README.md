# Historical Currency Rates Fetcher

Fetch and store **daily historical exchange rates** for the 20 most-used world currencies, using the [FreeCurrencyAPI](https://freecurrencyapi.com/docs).  
The script saves each month’s data as a CSV file (e.g. `2025-01.csv`), and runs automatically via cron inside a Docker container.

## Features

- Fetches daily exchange rates for:  
  > USD, EUR, GBP, JPY, CNY, INR, AUD, CAD, CHF, SEK, NOK, DKK,  
  > SGD, HKD, KRW, ZAR, BRL, MXN, TRY  
- Uses **USD** as the base currency  
- **Respects API rate limits** (10 requests/min and monthly quota)  
- **Automatically resumes** from where it left off  
- Saves **one CSV per month** in `/data`  
- Configurable through `.env`  
- Built-in **cron job** runs inside Docker  
- Supports optional **partial month retrieval**

## How It Works

1. The script checks the FreeCurrencyAPI `/status` endpoint to see remaining quota.  
2. It looks at which months and days already exist in your `/data` folder.  
3. If enough quota remains:
   - Fetches missing daily rates (1 request per day).
   - Waits automatically to respect 10 req/min limit.
   - Saves/updates the corresponding month’s CSV.
4. If not enough quota remains, it stops gracefully and tries again at the next cron run.

## Running with Docker

```bash
# 1. Clone this repo
git clone git@github.com:SizBlast/docker-historical-currency-rates-fetcher.git
cd docker-historical-currency-rates-fetcher

# 2. Copy the example env file and edit it
cp .env.example .env
# -> set your FREECURRENCY_API_KEY and desired cron schedule

# 3. Build and start the container
docker compose up -d
```

The container will automatically:

- Install dependencies
- Configure cron (from your .env)
- Start fetching data at the scheduled times

You can also use these commands with Docker:

```bash
# View logs in realtime
docker logs -f historical-currency-rates-fetcher

# Get shell access to the container
docker exec -it historical-currency-rates-fetcher
```

## `.env` Configuration

| Variable               | Description                   | Example                            |
|------------------------|-------------------------------|------------------------------------|
| `FREECURRENCY_API_KEY` | Your FreeCurrencyAPI key      | `your_api_key_here`                |
| `BASE_CURRENCY`        | Base currency for conversion  | `USD`                              |
| `DATA_DIR`             | Directory to store CSVs       | `/data`                            |
| `START_YEAR`           | First year to backfill        | `2015`                             |
| `ALLOW_PARTIAL_MONTH`  | Allow partial month retrieval | `true` / `false`                   |
| `CRON_SCHEDULE`        | Cron schedule for execution   | `"0 3 * * *"` (every day at 03:00) |
| `LOG_LEVEL`            | Optional logging level        | `INFO` / `DEBUG`                   |

## CSV Format

Each CSV file (`YYYY-MM.csv`) contains:

```csv
date,USD,EUR,GBP,JPY,CNY,INR,...
2025-01-01,1.0,0.92,0.79,150.23,7.12,83.05,...
2025-01-02,1.0,0.91,0.78,151.00,7.10,83.00,...
```

The base currency (USD) is always 1.0.