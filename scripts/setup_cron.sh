#!/bin/bash
# Add trading_accounts Redis sync cron (every 30 min)
CRON_LINE="*/30 * * * * /opt/mt5bridge/venv/bin/python /opt/mt5bridge/sync_trading_accounts_to_redis.py >> /var/log/trading_accounts_redis_sync.log 2>&1"

# Check if already exists
if crontab -l 2>/dev/null | grep -q "sync_trading_accounts_to_redis"; then
    echo "Cron job already exists"
else
    (crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
    echo "Cron job added"
fi

echo "Current crontab:"
crontab -l
