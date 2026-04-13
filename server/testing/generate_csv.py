import csv
import random
import string
import os
import sys
import time
from datetime import datetime, timedelta

TARGET_GB = 6       # Change to 10 for 10GB
OUTPUT_FILE = f"test_data_{TARGET_GB}gb.csv"

TARGET_BYTES = TARGET_GB * 1024 * 1024 * 1024

COLUMNS = [
    "id", "user_id", "name", "email", "phone",
    "amount", "currency", "status", "category",
    "description", "created_at", "updated_at",
    "region", "country", "ip_address", "session_id"
]

STATUSES   = ["active", "inactive", "pending", "failed", "completed"]
CATEGORIES = ["finance", "retail", "healthcare", "tech", "logistics", "education"]
REGIONS    = ["us-east", "us-west", "eu-central", "ap-south", "ap-northeast"]
COUNTRIES  = ["IN", "US", "GB", "DE", "SG", "AU", "JP", "FR", "CA", "BR"]
CURRENCIES = ["INR", "USD", "GBP", "EUR", "SGD", "AUD", "JPY"]

def random_string(n=8):
    return ''.join(random.choices(string.ascii_lowercase, k=n))

def random_ip():
    return f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"

def random_date():
    base = datetime(2022, 1, 1)
    delta = timedelta(days=random.randint(0, 1000), seconds=random.randint(0, 86400))
    return (base + delta).strftime("%Y-%m-%d %H:%M:%S")

def make_row(i):
    created = random_date()
    return [
        i,
        random.randint(1000, 9999999),
        f"{random_string(5)} {random_string(7)}",
        f"{random_string(6)}@{random_string(5)}.com",
        f"+91{random.randint(7000000000, 9999999999)}",
        round(random.uniform(1.0, 99999.99), 2),
        random.choice(CURRENCIES),
        random.choice(STATUSES),
        random.choice(CATEGORIES),
        f"{random_string(10)} {random_string(8)} {random_string(6)}",
        created,
        random_date(),
        random.choice(REGIONS),
        random.choice(COUNTRIES),
        random_ip(),
        f"sess_{random_string(16)}"
    ]

def main():
    print(f"Generating {TARGET_GB}GB CSV → {OUTPUT_FILE}")
    start = time.time()
    rows_written = 0
    last_report = 0

    with open(OUTPUT_FILE, "w", newline="", buffering=8 * 1024 * 1024) as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)

        while True:
            batch = [make_row(rows_written + j) for j in range(5000)]
            writer.writerows(batch)
            rows_written += 5000

            size = f.tell()
            pct = (size / TARGET_BYTES) * 100

            if pct - last_report >= 5:
                elapsed = time.time() - start
                speed_mb = (size / 1024 / 1024) / elapsed
                eta = (TARGET_BYTES - size) / (speed_mb * 1024 * 1024) if speed_mb > 0 else 0
                print(f"  {pct:.1f}%  |  {size/1024/1024:.0f} MB  |  {speed_mb:.1f} MB/s  |  ETA {eta:.0f}s")
                last_report = pct

            if size >= TARGET_BYTES:
                break

    final_size = os.path.getsize(OUTPUT_FILE)
    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.1f}s")
    print(f"File : {OUTPUT_FILE}")
    print(f"Size : {final_size / 1024 / 1024 / 1024:.2f} GB")
    print(f"Rows : {rows_written:,}")

if __name__ == "__main__":
    main()