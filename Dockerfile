FROM python:3.11-slim

# install cron and basic tools
RUN apt-get update \
  && apt-get install -y --no-install-recommends cron ca-certificates gcc nano \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# copy app files
COPY fetch_rates.py /app/fetch_rates.py
COPY run-cron.sh /app/run-cron.sh
COPY requirements.txt /app/requirements.txt

RUN chmod +x /app/fetch_rates.py /app/run-cron.sh

# install python deps
RUN pip install --no-cache-dir -r /app/requirements.txt

# create data dir
RUN mkdir -p /data && chown -R root:root /data

# Default command runs the cron wrapper script
CMD ["/bin/bash", "/app/run-cron.sh"]