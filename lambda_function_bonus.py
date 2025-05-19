import boto3
import psycopg2
import pandas as pd
import os
from datetime import datetime, timedelta
import io
import json

# AWS clients
s3 = boto3.client('s3')
secrets = boto3.client('secretsmanager')


def get_secret(secret_name):
    response = secrets.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])


def run_query():
    creds = get_secret("protecht-data")

    conn = psycopg2.connect(
        dbname=creds['dbname'],
        user=creds['username'],
        password=creds['password'],
        host=creds['host'],
        port=creds['port']
    )

    query = """
    WITH ranked_rates AS (
        SELECT currency_symbol, rate_date, exchange_rate,
               LAG(exchange_rate) OVER (PARTITION BY currency_symbol ORDER BY rate_date) AS prev_rate
        FROM exchange_rates
    ),
    diffs AS (
        SELECT *, CASE WHEN exchange_rate > prev_rate THEN 1 ELSE 0 END AS is_up
        FROM ranked_rates
    ),
    streaks AS (
        SELECT *, SUM(CASE WHEN is_up = 0 THEN 1 ELSE 0 END)
                  OVER (PARTITION BY currency_symbol ORDER BY rate_date) AS streak_group
        FROM diffs
    ),
    grouped AS (
        SELECT currency_symbol, streak_group,
               COUNT(*) AS streak_len,
               MAX(exchange_rate)/MIN(exchange_rate) - 1 AS perc_change
        FROM streaks
        WHERE is_up = 1
        GROUP BY currency_symbol, streak_group
        HAVING COUNT(*) >= 2
    ),
    agg_metrics AS (
        SELECT currency_symbol,
               AVG(streak_len) AS avg_cons_pos_days,
               AVG(perc_change) * 100 AS avg_cons_perc_change
        FROM grouped
        GROUP BY currency_symbol
    ),
    ranked AS (
        SELECT *, 
               RANK() OVER (ORDER BY avg_cons_pos_days DESC) AS avg_cons_pos_days_rank,
               RANK() OVER (ORDER BY avg_cons_perc_change DESC) AS avg_cons_perc_change_rank
        FROM agg_metrics
    )
    SELECT currency_symbol, avg_cons_pos_days, avg_cons_perc_change,
           avg_cons_pos_days_rank, avg_cons_perc_change_rank
    FROM ranked
    WHERE avg_cons_perc_change_rank <= 10;
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df


def lambda_handler(event, context):
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    bucket = "your-bucket"
    today_key = f"reports/{today}.csv"
    yesterday_key = f"reports/{yesterday}.csv"

    today_df = run_query()

    try:
        response = s3.get_object(Bucket=bucket, Key=yesterday_key)
        yesterday_df = pd.read_csv(response['Body'])
        yesterday_df = yesterday_df[['currency_symbol', 'avg_cons_perc_change_rank']]
        yesterday_df = yesterday_df.rename(columns={'avg_cons_perc_change_rank': 'prev_day_rank'})
    except s3.exceptions.NoSuchKey:
        yesterday_df = pd.DataFrame(columns=['currency_symbol', 'prev_day_rank'])

    final_df = today_df.merge(yesterday_df, on='currency_symbol', how='left')
    final_df['prev_day_rank'] = final_df['prev_day_rank'].fillna('N/A')

    # Upload today's CSV to S3
    csv_buffer = io.StringIO()
    final_df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket='jj-protecht-data',
        Key=today_key,
        Body=csv_buffer.getvalue()
    )

    return {
        'statusCode': 200,
        'body': f"Report written to s3://{bucket}/{today_key}"
    }

