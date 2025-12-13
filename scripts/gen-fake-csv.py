#!/usr/bin/env python
import argparse
import csv
import os
import random
from faker import Faker

fake = Faker()

SUPPORTED_TYPES = {
    "int32",
    "int64",
    "float64",
    "bool",
    "string",
    "date",
    "timestamp(ms)",
}

# -------------------------------------------------
# Name-based semantic inference
# -------------------------------------------------


def lname(name):
    return name.lower()


# ---- numeric inference ----


def is_money_field(name):
    return any(
        k in lname(name)
        for k in [
            "amount",
            "price",
            "total",
            "cost",
            "balance",
            "fee",
            "salary",
            "revenue",
            "income",
            "expense",
        ]
    )


def is_ratio_field(name):
    return any(k in lname(name) for k in ["rate", "ratio", "percent", "pct", "share"])


def infer_int_range(name):
    n = lname(name)

    if "age" in n:
        return 0, 100

    if is_money_field(name):
        return 0, 10_000

    if any(k in n for k in ["count", "qty", "quantity", "num", "items"]):
        return 0, 1_000

    if any(k in n for k in ["rank", "level", "score"]):
        return 1, 10

    if "year" in n:
        return 1990, 2030

    return 0, 10_000


def infer_float_range(name):
    if is_ratio_field(name):
        return 0.0, 1.0

    if is_money_field(name):
        return 0.0, 10_000.0

    return 0.0, 1_000.0


# ---- string inference ----


def infer_string(name):
    n = lname(name)

    if n == "id" or n.endswith("_id"):
        return str(random.randint(1, 10_000))

    if "first_name" in n:
        return fake.first_name()

    if "last_name" in n:
        return fake.last_name()

    if "full_name" in n or n == "name":
        return fake.name()

    if "email" in n:
        return fake.email()

    if "username" in n or "user_name" in n:
        return fake.user_name()

    if "phone" in n or "mobile" in n:
        return fake.phone_number()

    if "city" in n:
        return fake.city()

    if "state" in n or "region" in n:
        return fake.state()

    if "country" in n:
        return fake.country()

    if "zip" in n or "postal" in n:
        return fake.postcode()

    if "address" in n:
        return fake.street_address()

    if "company" in n or "employer" in n:
        return fake.company()

    if "department" in n:
        return random.choice(["sales", "engineering", "hr", "finance", "support"])

    if "status" in n:
        return random.choice(["active", "inactive", "pending", "archived"])

    if "type" in n or "category" in n:
        return random.choice(["A", "B", "C"])

    if "code" in n:
        return fake.bothify(text="??-####").upper()

    if "currency" in n:
        return random.choice(["USD", "EUR", "GBP", "JPY"])

    if "description" in n or "comment" in n or "notes" in n:
        return fake.sentence(nb_words=8)

    if "url" in n or "link" in n:
        return fake.url()

    return fake.word()


# ---- boolean inference ----


def infer_bool(name):
    n = lname(name)

    if n.startswith(("is_", "has_", "can_", "should_")):
        return random.random() < 0.7

    if "active" in n or "enabled" in n:
        return random.random() < 0.8

    return random.choice([True, False])


# ---- date / timestamp inference ----


def infer_date(name):
    n = lname(name)

    if "birth" in n:
        return fake.date_of_birth(minimum_age=0, maximum_age=100)

    if "start" in n:
        return fake.date_between(start_date="-3y", end_date="-30d")

    if "end" in n:
        return fake.date_between(start_date="-30d", end_date="+1y")

    return fake.date_between(start_date="-5y", end_date="today")


def infer_timestamp_ms(name):
    n = lname(name)

    if "created" in n:
        dt = fake.date_time_between(start_date="-3y", end_date="-1d")

    elif "updated" in n or "modified" in n:
        dt = fake.date_time_between(start_date="-30d", end_date="now")

    else:
        dt = fake.date_time_between(start_date="-5y", end_date="now")

    return int(dt.timestamp() * 1000)


# -------------------------------------------------
# Schema parsing
# -------------------------------------------------


def parse_schema(schema_path):
    columns = []

    with open(schema_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or ":" not in line:
                raise ValueError(f"Invalid schema line: {line}")

            name, rest = line.split(":", 1)
            parts = rest.strip().split()

            col_type = parts[0]
            if col_type not in SUPPORTED_TYPES:
                raise ValueError(f"Unsupported type: {col_type}")

            columns.append(
                {
                    "name": name.strip(),
                    "type": col_type,
                    "nullable": "nullable" in parts,
                    "key": "key" in parts,
                }
            )

    return columns


# -------------------------------------------------
# Value generation
# -------------------------------------------------


def fake_value(col, key_counters):
    if col["nullable"] and random.random() < 0.1:
        return None

    name = col["name"]
    t = col["type"]

    if col["key"]:
        key_counters[name] += 1
        return key_counters[name]

    if t in ("int32", "int64"):
        lo, hi = infer_int_range(name)
        return random.randint(lo, hi)

    if t == "float64":
        lo, hi = infer_float_range(name)
        if is_money_field(name):
            return round(random.uniform(lo, hi), 2)
        return round(random.uniform(lo, hi), 3)

    if t == "bool":
        return infer_bool(name)

    if t == "string":
        return infer_string(name)

    if t == "date":
        return infer_date(name).isoformat()

    if t == "timestamp(ms)":
        return infer_timestamp_ms(name)

    raise ValueError(f"Unknown type: {t}")


# -------------------------------------------------
# CSV generation
# -------------------------------------------------


def generate_csv(schema_path, rows, output_path):
    columns = parse_schema(schema_path)
    key_counters = {c["name"]: 0 for c in columns if c["key"]}

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([c["name"] for c in columns])

        for _ in range(rows):
            row = [fake_value(col, key_counters) for col in columns]
            writer.writerow(row)

    print(f"Generated {rows} rows → {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate fake CSV data from SSF schema"
    )
    parser.add_argument("schema", help="Path to _schema.ssf file")
    parser.add_argument("--rows", type=int, default=100)
    parser.add_argument("--output", help="Output CSV path")

    args = parser.parse_args()
    output = args.output or os.path.splitext(args.schema)[0] + ".csv"

    generate_csv(args.schema, args.rows, output)


if __name__ == "__main__":
    main()
