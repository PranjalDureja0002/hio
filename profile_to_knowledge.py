"""
Profile-to-Knowledge Converter
================================
Converts the data_profile.json (from db_profiler.py) into knowledge files
that the Knowledge Processor can consume directly.

Generates:
  1. column_values_profiled.txt  — column value hints (existing format)
  2. data_context.txt            — domain context with relationships, ranges, filters

Usage:
    python profile_to_knowledge.py --input data_profile.json --output-dir ./knowledge_output

    # Then upload column_values_profiled.txt and data_context.txt to your Knowledge Base
"""

import argparse
import json
import os
import sys


def load_profile(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def generate_column_values(profile):
    """Generate column_values_profiled.txt — compatible with Knowledge Processor's column_values_file format."""
    lines = []
    lines.append("# Auto-generated from database profiler")
    lines.append(f"# View: {profile['metadata']['view']}")
    lines.append(f"# Profiled: {profile['metadata']['profiled_at']}")
    lines.append(f"# Total rows: {profile['metadata']['total_rows']:,}")
    lines.append("")
    lines.append("column_values:")

    for col_name, col_info in profile["columns"].items():
        values = col_info.get("values", [])
        distinct_count = col_info.get("distinct_count", 0)
        null_pct = col_info.get("null_pct", 0)
        profile_type = col_info.get("profile_type", "")

        # Skip numeric-only and date-only columns (no value examples needed)
        if profile_type in ("numeric", "date") and not values:
            continue

        if not values:
            continue

        lines.append(f"  {col_name}:")
        lines.append(f"    cardinality: {distinct_count}")

        if null_pct > 0:
            lines.append(f"    null_pct: {null_pct}")

        if profile_type == "complete":
            lines.append(f"    complete: true")

        # Write examples
        lines.append(f"    examples:")
        for v in values:
            val = v["value"]
            if val is not None:
                lines.append(f"      - \"{val}\"")

    return "\n".join(lines)


def generate_data_context(profile):
    """Generate data_context.txt — free-text domain context for the LLM prompt."""
    meta = profile["metadata"]
    columns = profile["columns"]
    relationships = profile.get("relationships", [])

    lines = []
    lines.append(f"Data Profile (auto-generated from {meta['view']}, {meta['profiled_at'][:10]}):")
    lines.append(f"Total rows in scope: {meta['total_rows']:,} (filtered: {meta.get('date_filter', '')})")
    lines.append("")

    # ── Filter columns (low cardinality — use exact values) ──
    lines.append("FILTER COLUMNS — Use these EXACT values in WHERE clauses:")
    filter_cols = []
    for col_name, info in columns.items():
        if info.get("profile_type") == "complete":
            values = info.get("values", [])
            distinct = info.get("distinct_count", len(values))
            null_pct = info.get("null_pct", 0)
            fmt = info.get("format", "")
            val_list = [v["value"] for v in values if v["value"] is not None]

            # Format hint
            format_note = ""
            if fmt == "code":
                format_note = ", values are SHORT CODES"
            elif fmt == "uppercase":
                format_note = ", values are UPPERCASE"

            null_note = f", {null_pct}% NULL" if null_pct > 5 else ""

            if len(val_list) <= 20:
                val_str = ", ".join(f'"{v}"' for v in val_list)
                lines.append(f'  {col_name} ({distinct} values{format_note}{null_note}): {val_str}')
            else:
                # Too many to list inline — show first 15 + count
                val_str = ", ".join(f'"{v}"' for v in val_list[:15])
                lines.append(f'  {col_name} ({distinct} values{format_note}{null_note}): {val_str}, ... (+{len(val_list)-15} more)')
            filter_cols.append(col_name)

    lines.append("")

    # ── High-cardinality columns (top values only) ──
    high_card = [(n, i) for n, i in columns.items() if i.get("profile_type") == "top_n"]
    if high_card:
        lines.append("HIGH-CARDINALITY COLUMNS — Top values by frequency:")
        for col_name, info in high_card:
            values = info.get("values", [])[:10]
            distinct = info.get("distinct_count", 0)
            null_pct = info.get("null_pct", 0)
            null_note = f", {null_pct}% NULL" if null_pct > 5 else ""
            val_str = ", ".join(f'"{v["value"]}"' for v in values if v["value"])
            lines.append(f'  {col_name} ({distinct:,} distinct{null_note}): {val_str}, ...')
        lines.append("")

    # ── Numeric ranges ──
    numeric_cols = [(n, i) for n, i in columns.items() if i.get("profile_type") == "numeric"]
    if numeric_cols:
        lines.append("NUMERIC RANGES:")
        for col_name, info in numeric_cols:
            mn = info.get("min", "?")
            mx = info.get("max", "?")
            avg = info.get("avg", "?")
            p50 = info.get("p50", "?")
            null_pct = info.get("null_pct", 0)
            null_note = f" ({null_pct}% NULL)" if null_pct > 5 else ""
            # Format large numbers
            if isinstance(mn, (int, float)) and isinstance(mx, (int, float)):
                lines.append(f"  {col_name}: {mn:,.2f} to {mx:,.2f} (avg: {avg:,.2f}, median: {p50:,.2f}){null_note}")
            else:
                lines.append(f"  {col_name}: {mn} to {mx} (avg: {avg}){null_note}")
        lines.append("")

    # ── Date ranges ──
    date_cols = [(n, i) for n, i in columns.items() if i.get("profile_type") == "date"]
    if date_cols:
        lines.append("DATE RANGES:")
        for col_name, info in date_cols:
            lines.append(f"  {col_name}: {info.get('min', '?')} to {info.get('max', '?')}")
        lines.append("")

    # ── Functional dependencies ──
    if relationships:
        lines.append("COLUMN RELATIONSHIPS (functional dependencies — code → name, always paired):")
        for rel in relationships:
            cols = rel["columns"]
            lines.append(f"  {rel['direction']} — When displaying, SELECT both. Use code for filtering, name for display.")
        lines.append("")

    # ── High-NULL columns ──
    high_null = [(n, i["null_pct"]) for n, i in columns.items() if i.get("null_pct", 0) > 20]
    high_null.sort(key=lambda x: x[1], reverse=True)
    if high_null:
        lines.append("HIGH-NULL COLUMNS — Use NVL() or filter with IS NOT NULL:")
        for col_name, pct in high_null:
            lines.append(f"  {col_name}: {pct}% NULL")
        lines.append("")

    # ── Value format warnings ──
    code_cols = [n for n, i in columns.items() if i.get("format") == "code" and i.get("profile_type") == "complete"]
    if code_cols:
        lines.append("CODE COLUMNS — These use short codes, NOT full names (e.g. 'DE' not 'Germany'):")
        lines.append(f"  {', '.join(code_cols)}")
        lines.append("  ALWAYS use UPPER() for case-insensitive matching on these columns.")
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Convert data profile to knowledge files")
    parser.add_argument("--input", required=True, help="Path to data_profile.json")
    parser.add_argument("--output-dir", default=".", help="Output directory for knowledge files")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"Error: {args.input} not found")
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Loading profile from {args.input}...")
    profile = load_profile(args.input)

    meta = profile["metadata"]
    print(f"  View: {meta['view']}")
    print(f"  Columns: {meta['column_count']}")
    print(f"  Rows: {meta['total_rows']:,}")

    # Generate file 1: column_values_profiled.txt
    cv_content = generate_column_values(profile)
    cv_path = os.path.join(args.output_dir, "column_values_profiled.txt")
    with open(cv_path, "w", encoding="utf-8") as f:
        f.write(cv_content)
    cv_size = round(len(cv_content) / 1024, 1)
    print(f"\nGenerated: {cv_path} ({cv_size} KB)")

    # Generate file 2: data_context.txt
    dc_content = generate_data_context(profile)
    dc_path = os.path.join(args.output_dir, "data_context.txt")
    with open(dc_path, "w", encoding="utf-8") as f:
        f.write(dc_content)
    dc_size = round(len(dc_content) / 1024, 1)
    print(f"Generated: {dc_path} ({dc_size} KB)")

    # Summary
    cols = profile["columns"]
    complete = sum(1 for c in cols.values() if c.get("profile_type") == "complete")
    top_n = sum(1 for c in cols.values() if c.get("profile_type") == "top_n")
    numeric = sum(1 for c in cols.values() if c.get("profile_type") == "numeric")
    rels = len(profile.get("relationships", []))

    print(f"\nSummary:")
    print(f"  {complete} filter columns with ALL values listed (complete)")
    print(f"  {top_n} high-cardinality columns with top-{TOP_N_VALUES} values")
    print(f"  {numeric} numeric columns with ranges")
    print(f"  {rels} functional dependencies")
    print(f"\nUpload both files to your Knowledge Base:")
    print(f"  1. {cv_path}")
    print(f"  2. {dc_path}")


TOP_N_VALUES = 200

if __name__ == "__main__":
    main()
