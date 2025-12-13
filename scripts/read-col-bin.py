#!/usr/bin/env python3
import argparse
import struct
from datetime import date, timedelta

SUPPORTED_TYPES = {
    "int32",
    "int64",
    "float64",
    "bool",
    "string",
    "date",
    "timestamp(ms)",
}


def parse_schema_line(line):
    line = line.strip()
    if not line or ":" not in line:
        raise ValueError(f"Invalid schema line: {line}")

    name, rest = line.split(":", 1)
    parts = rest.strip().split()

    col_type = parts[0]
    if col_type not in SUPPORTED_TYPES:
        raise ValueError(f"Unsupported type: {col_type}")

    return {
        "name": name.strip(),
        "type": col_type,
        "nullable": "nullable" in parts,
        "key": "key" in parts,
    }


def parse_schema_file(schema_path):
    columns = []
    with open(schema_path, "r") as f:
        for line in f:
            columns.append(parse_schema_line(line))
    return {col["name"]: col for col in columns}


def read_exact(f, n):
    b = f.read(n)
    if len(b) != n:
        raise EOFError("Unexpected end of file")
    return b


def decode_value(f, col):
    """
    Reads one value from the BIN file according to schema encoding.
    """
    is_present = read_exact(f, 1)[0]
    if is_present == 0:
        return None

    t = col["type"]

    if t == "int32":
        return struct.unpack("<i", read_exact(f, 4))[0]

    if t == "int64":
        return struct.unpack("<q", read_exact(f, 8))[0]

    if t == "float64":
        return struct.unpack("<d", read_exact(f, 8))[0]

    if t == "bool":
        return bool(read_exact(f, 1)[0])

    if t == "string":
        length = struct.unpack("<I", read_exact(f, 4))[0]
        return read_exact(f, length).decode("utf-8")

    if t == "date":
        days = struct.unpack("<i", read_exact(f, 4))[0]
        epoch = date(1970, 1, 1)
        return epoch + timedelta(days=days)

    if t == "timestamp(ms)":
        return struct.unpack("<q", read_exact(f, 8))[0]

    raise ValueError(f"Unknown type: {t}")


def main():
    parser = argparse.ArgumentParser(description="Read a column BIN file in text form")
    parser.add_argument("--schema", required=True, help="Path to SSF schema file")
    parser.add_argument("--bin", required=True, help="Path to a column BIN file")
    parser.add_argument("--column", required=True, help="Column name")

    args = parser.parse_args()

    schema = parse_schema_file(args.schema)

    if args.column not in schema:
        raise ValueError(f"Column '{args.column}' not found in schema")

    col = schema[args.column]

    with open(args.bin, "rb") as f:
        row = 0
        while True:
            try:
                value = decode_value(f, col)
            except EOFError:
                break

            if value is None:
                print("NULL")
            else:
                print(value)

            row += 1


if __name__ == "__main__":
    main()
