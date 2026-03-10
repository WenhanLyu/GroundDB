#!/usr/bin/env python3
"""
Pure-Python TPC-H data generator for Scale Factor 0.01.

Generates all 8 TPC-H tables as pipe-delimited .tbl files.
Deterministic pseudo-random generation using a fixed seed.

Usage:
    python scripts/generate_tpch.py [output_dir]
    
Default output directory: data/
"""

import os
import sys
import random
from datetime import date, timedelta

# Fixed seed for reproducibility
SEED = 42

# Scale factor
SF = 0.01

# TPC-H row counts at SF=1
BASE_COUNTS = {
    "region": 5,        # Fixed
    "nation": 25,       # Fixed
    "supplier": 10000,
    "customer": 150000,
    "part": 200000,
    "partsupp": 800000,
    "orders": 1500000,
    "lineitem": 6000000,  # ~4 per order average
}


def scale_count(table_name: str, sf: float) -> int:
    """Get row count for a given SF."""
    base = BASE_COUNTS[table_name]
    if table_name in ("region", "nation"):
        return base  # Fixed tables
    return max(1, int(base * sf))


# ── Reference data ──────────────────────────────────────────────────────────

REGIONS = [
    (0, "AFRICA"),
    (1, "AMERICA"),
    (2, "ASIA"),
    (3, "EUROPE"),
    (4, "MIDDLE EAST"),
]

NATIONS = [
    (0, "ALGERIA", 0), (1, "ARGENTINA", 1), (2, "BRAZIL", 1),
    (3, "CANADA", 1), (4, "EGYPT", 4), (5, "ETHIOPIA", 0),
    (6, "FRANCE", 3), (7, "GERMANY", 3), (8, "INDIA", 2),
    (9, "INDONESIA", 2), (10, "IRAN", 4), (11, "IRAQ", 4),
    (12, "JAPAN", 2), (13, "JORDAN", 4), (14, "KENYA", 0),
    (15, "MOROCCO", 0), (16, "MOZAMBIQUE", 0), (17, "PERU", 1),
    (18, "CHINA", 2), (19, "ROMANIA", 3), (20, "SAUDI ARABIA", 4),
    (21, "VIETNAM", 2), (22, "RUSSIA", 3), (23, "UNITED KINGDOM", 3),
    (24, "UNITED STATES", 1),
]

MKTSEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
SHIP_INSTRUCT = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
SHIP_MODES = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
RETURN_FLAGS = ["R", "A", "N"]
LINE_STATUS = ["O", "F"]
ORDER_STATUS = ["O", "F", "P"]

TYPES_SYLLABLE1 = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
TYPES_SYLLABLE2 = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
TYPES_SYLLABLE3 = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]

CONTAINERS_SYLLABLE1 = ["SM", "MED", "LG", "WRAP", "JUMBO"]
CONTAINERS_SYLLABLE2 = ["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"]

BRANDS = [f"Brand#{i}{j}" for i in range(1, 6) for j in range(1, 6)]

# Comments - simple word lists
NOUNS = ["foxes", "ideas", "theodolites", "pinto", "beans", "instructions",
         "dependencies", "excuses", "platelets", "asymptotes", "courts",
         "dolphins", "multipliers", "sauternes", "warthogs", "frets",
         "dinos", "attainments", "somas", "Tiresias", "patterns",
         "forges", "braids", "hockey", "players", "frays", "warhorses"]
VERBS = ["sleep", "wake", "are", "cajole", "haggle", "nag", "use",
         "boost", "affix", "detect", "integrate", "maintain", "nod",
         "was", "lose", "sublate", "solve", "thrash", "promise",
         "engage", "hinder", "print", "x-ray", "breach", "eat"]
ADJECTIVES = ["furious", "sly", "careful", "blithe", "quick", "fluffy",
              "slow", "quiet", "ruthless", "thin", "close", "dogged",
              "daring", "brave", "stealthy", "permanent", "enticing",
              "idle", "busy", "regular", "final", "ironic", "even",
              "bold", "silent"]
ADVERBS = ["sometimes", "always", "never", "furiously", "slyly",
           "carefully", "blithely", "quickly", "fluffily", "slowly",
           "quietly", "ruthlessly", "thinly", "closely", "doggedly",
           "daringly", "bravely", "stealthily", "permanently", "enticingly",
           "idly", "busily", "regularly", "finally", "ironically"]


def random_comment(rng: random.Random, min_len=10, max_len=40) -> str:
    """Generate a random TPC-H-style comment."""
    words = []
    length = 0
    target = rng.randint(min_len, max_len)
    while length < target:
        word = rng.choice(NOUNS + VERBS + ADJECTIVES + ADVERBS)
        words.append(word)
        length += len(word) + 1
    return " ".join(words)


def random_phone(rng: random.Random, nation_key: int) -> str:
    """Generate a phone number: CC-AAA-BBB-CCCC."""
    cc = 10 + nation_key
    return f"{cc}-{rng.randint(100,999)}-{rng.randint(100,999)}-{rng.randint(1000,9999)}"


def random_address(rng: random.Random) -> str:
    """Generate a random address string."""
    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ,."
    length = rng.randint(10, 25)
    return "".join(rng.choice(chars) for _ in range(length))


def random_date(rng: random.Random, start: date, end: date) -> date:
    """Random date between start and end (inclusive)."""
    delta = (end - start).days
    return start + timedelta(days=rng.randint(0, delta))


# ── Generators for each table ───────────────────────────────────────────────

def generate_regions(output_dir: str):
    """Generate region.tbl (fixed 5 rows)."""
    rng = random.Random(SEED)
    with open(os.path.join(output_dir, "region.tbl"), "w") as f:
        for rk, rname in REGIONS:
            comment = random_comment(rng)
            f.write(f"{rk}|{rname}|{comment}|\n")


def generate_nations(output_dir: str):
    """Generate nation.tbl (fixed 25 rows)."""
    rng = random.Random(SEED + 1)
    with open(os.path.join(output_dir, "nation.tbl"), "w") as f:
        for nk, nname, nrk in NATIONS:
            comment = random_comment(rng)
            f.write(f"{nk}|{nname}|{nrk}|{comment}|\n")


def generate_suppliers(output_dir: str, count: int):
    """Generate supplier.tbl."""
    rng = random.Random(SEED + 2)
    with open(os.path.join(output_dir, "supplier.tbl"), "w") as f:
        for sk in range(1, count + 1):
            name = f"Supplier#{sk:09d}"
            address = random_address(rng)
            nationkey = rng.randint(0, 24)
            phone = random_phone(rng, nationkey)
            acctbal = round(rng.uniform(-999.99, 9999.99), 2)
            comment = random_comment(rng)
            f.write(f"{sk}|{name}|{address}|{nationkey}|{phone}|{acctbal:.2f}|{comment}|\n")


def generate_customers(output_dir: str, count: int):
    """Generate customer.tbl."""
    rng = random.Random(SEED + 3)
    with open(os.path.join(output_dir, "customer.tbl"), "w") as f:
        for ck in range(1, count + 1):
            name = f"Customer#{ck:09d}"
            address = random_address(rng)
            nationkey = rng.randint(0, 24)
            phone = random_phone(rng, nationkey)
            acctbal = round(rng.uniform(-999.99, 9999.99), 2)
            mktsegment = rng.choice(MKTSEGMENTS)
            comment = random_comment(rng)
            f.write(f"{ck}|{name}|{address}|{nationkey}|{phone}|{acctbal:.2f}|{mktsegment}|{comment}|\n")


def generate_parts(output_dir: str, count: int):
    """Generate part.tbl."""
    rng = random.Random(SEED + 4)
    with open(os.path.join(output_dir, "part.tbl"), "w") as f:
        for pk in range(1, count + 1):
            name = " ".join(rng.choice(ADJECTIVES) for _ in range(rng.randint(1, 3)))
            mfgr = f"Manufacturer#{rng.randint(1, 5)}"
            brand = rng.choice(BRANDS)
            ptype = f"{rng.choice(TYPES_SYLLABLE1)} {rng.choice(TYPES_SYLLABLE2)} {rng.choice(TYPES_SYLLABLE3)}"
            size = rng.randint(1, 50)
            container = f"{rng.choice(CONTAINERS_SYLLABLE1)} {rng.choice(CONTAINERS_SYLLABLE2)}"
            retailprice = round(90000 + (pk % 20000) * 1.0 + pk / 10.0, 2)  # TPC-H formula-ish
            comment = random_comment(rng)
            f.write(f"{pk}|{name}|{mfgr}|{brand}|{ptype}|{size}|{container}|{retailprice:.2f}|{comment}|\n")


def generate_partsupp(output_dir: str, part_count: int, supp_count: int):
    """Generate partsupp.tbl — 4 rows per part."""
    rng = random.Random(SEED + 5)
    with open(os.path.join(output_dir, "partsupp.tbl"), "w") as f:
        for pk in range(1, part_count + 1):
            for j in range(4):
                sk = ((pk + j * (supp_count // 4 + 1) - 1) % supp_count) + 1
                availqty = rng.randint(1, 9999)
                supplycost = round(rng.uniform(1.00, 1000.00), 2)
                comment = random_comment(rng, 49, 100)
                f.write(f"{pk}|{sk}|{availqty}|{supplycost:.2f}|{comment}|\n")


def generate_orders_and_lineitems(output_dir: str, order_count: int, cust_count: int):
    """Generate orders.tbl and lineitem.tbl."""
    rng = random.Random(SEED + 6)
    
    # Determine the part count and supplier count for referencing
    part_count = scale_count("part", SF)
    supp_count = scale_count("supplier", SF)

    order_date_start = date(1992, 1, 1)
    order_date_end = date(1998, 8, 2)

    fo = open(os.path.join(output_dir, "orders.tbl"), "w")
    fl = open(os.path.join(output_dir, "lineitem.tbl"), "w")

    try:
        for ok in range(1, order_count + 1):
            custkey = rng.randint(1, cust_count)
            order_date = random_date(rng, order_date_start, order_date_end)
            priority = rng.choice(PRIORITIES)
            clerk = f"Clerk#{rng.randint(1, max(1, int(1000 * SF))):09d}"
            shippriority = 0
            comment = random_comment(rng)

            # Generate 1-7 line items per order
            num_items = rng.randint(1, 7)
            total_price = 0.0
            statuses = []

            line_items = []
            for ln in range(1, num_items + 1):
                partkey = rng.randint(1, part_count)
                suppkey = rng.randint(1, supp_count)
                quantity = rng.randint(1, 50)
                extendedprice = round(quantity * rng.uniform(900.0, 100000.0) / 100.0, 2)
                discount = round(rng.randint(0, 10) / 100.0, 2)
                tax = round(rng.randint(0, 8) / 100.0, 2)

                ship_date = order_date + timedelta(days=rng.randint(1, 121))
                commit_date = order_date + timedelta(days=rng.randint(30, 90))
                receipt_date = ship_date + timedelta(days=rng.randint(1, 30))

                # Return flag depends on receipt date
                if receipt_date <= date(1995, 6, 17):
                    returnflag = rng.choice(["R", "A"])
                else:
                    returnflag = "N"

                # Line status depends on ship date
                if ship_date > date(1995, 6, 17):
                    linestatus = "O"
                else:
                    linestatus = "F"

                shipinstruct = rng.choice(SHIP_INSTRUCT)
                shipmode = rng.choice(SHIP_MODES)
                lcomment = random_comment(rng)

                total_price += extendedprice * (1 - discount) * (1 + tax)
                statuses.append(linestatus)

                line_items.append(
                    f"{ok}|{partkey}|{suppkey}|{ln}|{quantity:.2f}|{extendedprice:.2f}|"
                    f"{discount:.2f}|{tax:.2f}|{returnflag}|{linestatus}|"
                    f"{ship_date}|{commit_date}|{receipt_date}|"
                    f"{shipinstruct}|{shipmode}|{lcomment}|"
                )

            # Determine order status
            if all(s == "O" for s in statuses):
                orderstatus = "O"
            elif all(s == "F" for s in statuses):
                orderstatus = "F"
            else:
                orderstatus = "P"

            total_price = round(total_price, 2)

            fo.write(
                f"{ok}|{custkey}|{orderstatus}|{total_price:.2f}|{order_date}|"
                f"{priority}|{clerk}|{shippriority}|{comment}|\n"
            )

            for li in line_items:
                fl.write(li + "\n")

    finally:
        fo.close()
        fl.close()


def generate_all(output_dir: str = "data"):
    """Generate all TPC-H tables at SF 0.01."""
    os.makedirs(output_dir, exist_ok=True)

    supp_count = scale_count("supplier", SF)
    cust_count = scale_count("customer", SF)
    part_count = scale_count("part", SF)
    order_count = scale_count("orders", SF)

    print(f"Generating TPC-H data at SF={SF} into {output_dir}/")
    print(f"  Regions:   {scale_count('region', SF)}")
    print(f"  Nations:   {scale_count('nation', SF)}")
    print(f"  Suppliers: {supp_count}")
    print(f"  Customers: {cust_count}")
    print(f"  Parts:     {part_count}")
    print(f"  PartSupp:  {part_count * 4}")
    print(f"  Orders:    {order_count}")

    generate_regions(output_dir)
    generate_nations(output_dir)
    generate_suppliers(output_dir, supp_count)
    generate_customers(output_dir, cust_count)
    generate_parts(output_dir, part_count)
    generate_partsupp(output_dir, part_count, supp_count)
    generate_orders_and_lineitems(output_dir, order_count, cust_count)

    # Count lineitems
    lineitem_count = sum(1 for _ in open(os.path.join(output_dir, "lineitem.tbl")))
    print(f"  LineItems: {lineitem_count}")
    print("Done!")


if __name__ == "__main__":
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "data"
    generate_all(output_dir)
