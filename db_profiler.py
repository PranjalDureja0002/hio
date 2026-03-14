"""
Database Profiler for NL-to-SQL Pipeline
=========================================
Profiles an Oracle view/table and generates a comprehensive data profile JSON.
This profile feeds the Knowledge Processor to dramatically improve SQL generation accuracy.

Usage:
    python db_profiler.py --host <host> --port 1521 --service <service> \
                          --user <user> --password <pwd> \
                          --view VW_SPEND_REPORT_VIEW \
                          --output data_profile.json

    # Or with a .env / config file:
    python db_profiler.py --config db_config.json --view VW_SPEND_REPORT_VIEW

Requirements:
    pip install oracledb
"""

import argparse
import json
import time
import sys
from datetime import datetime


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# Mandatory date filter to avoid timeout on large datasets
DATE_FILTER = "INVOICE_DATE > DATE '2024-04-01'"

# Columns with distinct count below this get ALL values profiled
LOW_CARDINALITY_THRESHOLD = 200

# For high-cardinality columns, fetch top N values
TOP_N_VALUES = 200

# Known column pairs to check for functional dependencies
# Format: (parent_col, child_col) — checks if parent → child is 1:1
DEPENDENCY_CANDIDATES = [
    ("SUPPLIER_NO", "SUPPLIER_NAME"),
    ("PLANT_NO", "PLANT_NAME"),
    ("COMMODITY", "COMMODITY_DESCRIPTION"),
    ('"Material Group"', '"MG Description"'),
    ("MAIN_ACCOUNT", "Main_Account_Description"),
    ('"Main Plant No"', '"Main Plant Name"'),
    ("ARTICLE_NO", "ARTICLE_DESCRIPTION"),
    ('"Com. Supplier"', '"Com. Desr. Supp."'),
]

# Columns known to be numeric (for min/max/avg/percentile profiling)
NUMERIC_COLUMNS = {"AMOUNT", "QUANTITY", "EXCH_RATE", "VCHR_LOC_CURRENCY_AMT"}

# Columns known to be dates
DATE_COLUMNS = {"INVOICE_DATE"}


def _quote_col(col):
    """Quote a column name for Oracle SQL. Already-quoted names pass through."""
    if col.startswith('"'):
        return col
    # Columns with spaces, mixed case, or special chars need quoting
    if any(c in col for c in (" ", "(", ")", ".", "-")) or not col.isupper():
        return f'"{col}"'
    return col


def _unquote_col(col):
    """Remove quotes for display/dict keys."""
    if col.startswith('"') and col.endswith('"'):
        return col[1:-1]
    return col


# ═══════════════════════════════════════════════════════════════════════════════
# PROFILER
# ═══════════════════════════════════════════════════════════════════════════════

def profile_view(conn, view_name, date_filter=DATE_FILTER):
    """Profile all columns in a view/table."""
    cur = conn.cursor()
    cur.call_timeout = 120000  # 2 min per query

    print(f"\n{'='*60}")
    print(f"  Profiling: {view_name}")
    print(f"  Date filter: {date_filter}")
    print(f"{'='*60}\n")

    # ── Step 1: Get total row count ──
    print("[1/5] Counting rows...", end=" ", flush=True)
    cur.execute(f"SELECT COUNT(*) FROM {view_name} WHERE {date_filter}")
    total_rows = cur.fetchone()[0]
    print(f"{total_rows:,} rows")

    # ── Step 2: Get column list and types from Oracle metadata ──
    print("[2/5] Reading column metadata...", end=" ", flush=True)
    cur.execute(f"""
        SELECT column_name, data_type, data_length, nullable
        FROM ALL_TAB_COLUMNS
        WHERE table_name = :1
        ORDER BY column_id
    """, [view_name])
    col_meta = {}
    for row in cur.fetchall():
        col_meta[row[0]] = {
            "type": row[1],
            "length": row[2],
            "nullable": row[3] == "Y",
        }
    print(f"{len(col_meta)} columns")

    if not col_meta:
        # Try without schema qualification — view might have mixed-case name
        print("  (no columns found via ALL_TAB_COLUMNS, trying describe...)")
        cur.execute(f"SELECT * FROM {view_name} WHERE ROWNUM = 0")
        col_meta = {}
        for d in cur.description:
            col_meta[d[0]] = {"type": str(d[1]), "length": d[2], "nullable": True}
        print(f"  Found {len(col_meta)} columns via describe")

    # ── Step 3: Profile each column ──
    print(f"[3/5] Profiling {len(col_meta)} columns...")
    columns_profile = {}
    total_cols = len(col_meta)

    for idx, (col_name, meta) in enumerate(col_meta.items(), 1):
        qcol = _quote_col(col_name)
        display_name = _unquote_col(col_name) if col_name != qcol else col_name
        prefix = f"  [{idx}/{total_cols}] {display_name}"
        print(f"{prefix}...", end=" ", flush=True)

        profile = {
            "type": meta["type"],
            "nullable": meta["nullable"],
        }

        try:
            # Get cardinality and null count
            cur.execute(f"""
                SELECT COUNT(DISTINCT {qcol}),
                       SUM(CASE WHEN {qcol} IS NULL THEN 1 ELSE 0 END)
                FROM {view_name}
                WHERE {date_filter}
            """)
            row = cur.fetchone()
            distinct_count = row[0] or 0
            null_count = row[1] or 0
            null_pct = round((null_count / total_rows * 100), 1) if total_rows > 0 else 0

            profile["distinct_count"] = distinct_count
            profile["null_pct"] = null_pct

            # Decide profiling strategy
            col_upper = col_name.upper().replace('"', '')

            if col_upper in NUMERIC_COLUMNS or meta["type"] in ("NUMBER", "FLOAT", "BINARY_DOUBLE"):
                # Numeric profiling
                profile["profile_type"] = "numeric"
                try:
                    cur.execute(f"""
                        SELECT MIN({qcol}), MAX({qcol}), ROUND(AVG({qcol}), 2),
                               PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {qcol}),
                               PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {qcol}),
                               PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {qcol})
                        FROM {view_name}
                        WHERE {date_filter} AND {qcol} IS NOT NULL
                    """)
                    stats = cur.fetchone()
                    if stats:
                        profile["min"] = float(stats[0]) if stats[0] is not None else None
                        profile["max"] = float(stats[1]) if stats[1] is not None else None
                        profile["avg"] = float(stats[2]) if stats[2] is not None else None
                        profile["p25"] = float(stats[3]) if stats[3] is not None else None
                        profile["p50"] = float(stats[4]) if stats[4] is not None else None
                        profile["p75"] = float(stats[5]) if stats[5] is not None else None
                except Exception:
                    pass  # percentiles may fail on some types
                print(f"numeric, {distinct_count:,} distinct, range {profile.get('min','?')}-{profile.get('max','?')}")

            elif col_upper in DATE_COLUMNS or meta["type"] in ("DATE", "TIMESTAMP"):
                # Date profiling
                profile["profile_type"] = "date"
                try:
                    cur.execute(f"""
                        SELECT MIN({qcol}), MAX({qcol})
                        FROM {view_name}
                        WHERE {date_filter} AND {qcol} IS NOT NULL
                    """)
                    stats = cur.fetchone()
                    if stats:
                        profile["min"] = stats[0].strftime("%Y-%m-%d") if stats[0] else None
                        profile["max"] = stats[1].strftime("%Y-%m-%d") if stats[1] else None
                except Exception:
                    pass
                print(f"date, range {profile.get('min','?')} to {profile.get('max','?')}")

            elif distinct_count <= LOW_CARDINALITY_THRESHOLD:
                # Low cardinality — get ALL distinct values with counts
                profile["profile_type"] = "complete"
                cur.execute(f"""
                    SELECT {qcol}, COUNT(*) AS cnt
                    FROM {view_name}
                    WHERE {date_filter} AND {qcol} IS NOT NULL
                    GROUP BY {qcol}
                    ORDER BY cnt DESC
                """)
                values = []
                for vrow in cur.fetchall():
                    values.append({
                        "value": str(vrow[0]).strip() if vrow[0] is not None else None,
                        "count": vrow[1],
                        "pct": round(vrow[1] / total_rows * 100, 1) if total_rows > 0 else 0,
                    })
                profile["values"] = values
                print(f"complete, {distinct_count} values")

            else:
                # High cardinality — top N values
                profile["profile_type"] = "top_n"
                cur.execute(f"""
                    SELECT {qcol}, COUNT(*) AS cnt
                    FROM {view_name}
                    WHERE {date_filter} AND {qcol} IS NOT NULL
                    GROUP BY {qcol}
                    ORDER BY cnt DESC
                    FETCH FIRST {TOP_N_VALUES} ROWS ONLY
                """)
                values = []
                for vrow in cur.fetchall():
                    values.append({
                        "value": str(vrow[0]).strip() if vrow[0] is not None else None,
                        "count": vrow[1],
                        "pct": round(vrow[1] / total_rows * 100, 1) if total_rows > 0 else 0,
                    })
                profile["values"] = values
                print(f"top_{TOP_N_VALUES}, {distinct_count:,} distinct")

        except Exception as e:
            profile["error"] = str(e)[:200]
            print(f"ERROR: {e}")

        columns_profile[col_name] = profile

    # ── Step 4: Detect functional dependencies ──
    print(f"\n[4/5] Checking functional dependencies...")
    relationships = []
    for parent, child in DEPENDENCY_CANDIDATES:
        parent_display = _unquote_col(parent)
        child_display = _unquote_col(child)
        print(f"  {parent_display} → {child_display}...", end=" ", flush=True)
        try:
            # If every distinct value of parent maps to exactly one child value,
            # it's a functional dependency
            qp = _quote_col(parent) if not parent.startswith('"') else parent
            qc = _quote_col(child) if not child.startswith('"') else child
            cur.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {qp}, COUNT(DISTINCT {qc}) AS child_cnt
                    FROM {view_name}
                    WHERE {date_filter} AND {qp} IS NOT NULL AND {qc} IS NOT NULL
                    GROUP BY {qp}
                    HAVING COUNT(DISTINCT {qc}) > 1
                )
            """)
            violations = cur.fetchone()[0]
            if violations == 0:
                relationships.append({
                    "columns": [parent_display, child_display],
                    "type": "functional_dependency",
                    "direction": f"{parent_display} → {child_display}",
                })
                print("YES (1:1)")
            else:
                print(f"NO ({violations} violations)")
        except Exception as e:
            print(f"ERROR: {e}")

    # ── Step 5: Detect value format patterns ──
    print(f"\n[5/5] Analyzing value formats...")
    for col_name, profile in columns_profile.items():
        values = profile.get("values", [])
        if not values:
            continue
        sample_vals = [v["value"] for v in values[:20] if v["value"]]
        if not sample_vals:
            continue

        # Check format patterns
        all_upper = all(v == v.upper() for v in sample_vals)
        all_codes = all(len(v) <= 5 for v in sample_vals)
        has_spaces = any(" " in v for v in sample_vals)

        if all_upper and all_codes:
            profile["format"] = "code"
        elif all_upper:
            profile["format"] = "uppercase"
        elif has_spaces:
            profile["format"] = "text_with_spaces"
        else:
            profile["format"] = "mixed"

    cur.close()

    # ── Build final output ──
    result = {
        "metadata": {
            "view": view_name,
            "profiled_at": datetime.now().isoformat(),
            "total_rows": total_rows,
            "column_count": len(columns_profile),
            "date_filter": date_filter,
        },
        "columns": columns_profile,
        "relationships": relationships,
    }

    print(f"\n{'='*60}")
    print(f"  Done! {len(columns_profile)} columns profiled")
    print(f"  {sum(1 for c in columns_profile.values() if c.get('profile_type') == 'complete')} complete (all values)")
    print(f"  {sum(1 for c in columns_profile.values() if c.get('profile_type') == 'top_n')} top-N")
    print(f"  {sum(1 for c in columns_profile.values() if c.get('profile_type') == 'numeric')} numeric")
    print(f"  {sum(1 for c in columns_profile.values() if c.get('profile_type') == 'date')} date")
    print(f"  {len(relationships)} functional dependencies found")
    print(f"{'='*60}")

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Profile an Oracle view for NL-to-SQL pipeline")
    parser.add_argument("--host", required=True, help="Oracle host")
    parser.add_argument("--port", type=int, default=1521, help="Oracle port (default: 1521)")
    parser.add_argument("--service", required=True, help="Oracle service name")
    parser.add_argument("--user", required=True, help="Oracle username")
    parser.add_argument("--password", required=True, help="Oracle password")
    parser.add_argument("--view", default="VW_SPEND_REPORT_VIEW", help="View/table to profile")
    parser.add_argument("--output", default="data_profile.json", help="Output JSON file path")
    parser.add_argument("--date-filter", default=DATE_FILTER, help="Mandatory date filter")
    parser.add_argument("--config", help="JSON config file with connection params")

    args = parser.parse_args()

    # Load config file if provided
    if args.config:
        with open(args.config) as f:
            cfg = json.load(f)
        host = cfg.get("host", args.host)
        port = cfg.get("port", args.port)
        service = cfg.get("service", args.service)
        user = cfg.get("user", args.user)
        password = cfg.get("password", args.password)
    else:
        host, port, service = args.host, args.port, args.service
        user, password = args.user, args.password

    # Connect
    import oracledb
    print(f"Connecting to {host}:{port}/{service} as {user}...")
    dsn = oracledb.makedsn(host, port, service_name=service)
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    print("Connected.\n")

    try:
        start = time.time()
        result = profile_view(conn, args.view, args.date_filter)
        elapsed = round(time.time() - start, 1)
        result["metadata"]["profiling_seconds"] = elapsed

        # Write output
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False, default=str)

        print(f"\nProfile saved to: {args.output}")
        print(f"Total time: {elapsed}s")
        print(f"File size: {round(len(json.dumps(result)) / 1024, 1)} KB")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
