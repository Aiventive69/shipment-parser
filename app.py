"""
app.py
------
Flask API voor de shipment parser.
Make.com stuurt de bestandsinhoud als POST → krijgt JSON records terug.

Endpoint: POST /parse
Body (JSON): { "text": "...bestandsinhoud..." }
Response:    { "records": [ {...}, ... ], "count": 5 }
"""

import json
import logging
import re
from typing import Any, Dict, List, Tuple

from flask import Flask, jsonify, request

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG — pas dit aan als het bestand van je klant er anders uitziet
# ---------------------------------------------------------------------------

SHIPMENT_BLOCK_MARKER: str = r"^(SEA|AIR|ROAD|RAIL|SHIPMENT)\b"

K_NUMBER_PATTERN: str = r"\b(KM?\d{2,4}-\d{4,6})\b"

HEADER_FIELDS: List[Tuple[str, str, int]] = [
    ("container",    r"CONTAINER[:\s#]*([A-Z]{4}\d{7})",                1),
    ("vessel",       r"VESSEL[:\s]*([A-Z0-9 /\-]{3,40}?)(?:\s{2,}|$)", 1),
    ("voyage",       r"VOY(?:AGE)?[:\s#]*([A-Z0-9\-]+)",               1),
    ("pol",          r"POL[:\s]*([A-Z\s,]+?)(?:\s{2,}|$)",             1),
    ("pod",          r"POD[:\s]*([A-Z\s,]+?)(?:\s{2,}|$)",             1),
    ("etd",          r"ETD[:\s]*(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})", 1),
    ("eta",          r"ETA[:\s]*(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})", 1),
    ("booking_ref",  r"B(?:OOKING)?[:\s#]*([A-Z0-9\-]{4,20})",        1),
    ("shipper",      r"SHIPPER[:\s]*(.+?)(?:\s{2,}|$)",                1),
    ("consignee",    r"CONSIGNEE[:\s]*(.+?)(?:\s{2,}|$)",              1),
]

LINE_FIELDS: List[Tuple[str, str, int]] = [
    ("quantity",    r"(\d[\d,\.]*)\s*(CARTONS?|PALLETS?|PIECES?|PCS|CTNS?|PLT)", 1),
    ("unit",        r"(\d[\d,\.]*)\s*(CARTONS?|PALLETS?|PIECES?|PCS|CTNS?|PLT)", 2),
    ("weight_kg",   r"(\d[\d,\.]+)\s*KG",                              1),
    ("cbm",         r"(\d[\d,\.]+)\s*CBM",                             1),
    ("description", r"(?:DESCRIPTION|DESC|GOODS)[:\s]*(.+?)(?:\s{2,}|$)", 1),
]

# ---------------------------------------------------------------------------
# PARSER FUNCTIES (zelfde logica als shipment_parser.py)
# ---------------------------------------------------------------------------

def _search(pattern: str, text: str, group: int = 1) -> str:
    m = re.search(pattern, text, re.IGNORECASE)
    return m.group(group).strip() if m else ""

def _normalise_k(raw: str) -> str:
    return raw.upper().strip()

def split_shipments(text: str) -> List[str]:
    marker_re = re.compile(SHIPMENT_BLOCK_MARKER, re.IGNORECASE | re.MULTILINE)
    lines = text.splitlines(keepends=True)
    blocks: List[str] = []
    current: List[str] = []
    for line in lines:
        if marker_re.match(line.strip()) and current:
            blocks.append("".join(current))
            current = [line]
        else:
            current.append(line)
    if current:
        blocks.append("".join(current))
    k_re = re.compile(K_NUMBER_PATTERN, re.IGNORECASE)
    return [b for b in blocks if k_re.search(b)]

def extract_header(block: str) -> Dict[str, str]:
    header: Dict[str, str] = {key: "" for key, *_ in HEADER_FIELDS}
    for line in block.splitlines():
        line_upper = line.upper()
        for key, pattern, group in HEADER_FIELDS:
            if header[key]:
                continue
            val = _search(pattern, line_upper, group)
            if val:
                header[key] = val.title() if key in ("pol", "pod", "shipper", "consignee") else val
    return header

def extract_lines(block: str) -> List[Dict[str, Any]]:
    k_re = re.compile(K_NUMBER_PATTERN, re.IGNORECASE)
    results: List[Dict[str, Any]] = []
    for line in block.splitlines():
        m = k_re.search(line)
        if not m:
            continue
        record: Dict[str, Any] = {
            "k_number": _normalise_k(m.group(1)),
            "raw_line": line.strip(),
        }
        line_upper = line.upper()
        for key, pattern, group in LINE_FIELDS:
            record[key] = _search(pattern, line_upper, group)
        results.append(record)
    return results

def parse(text: str) -> List[Dict[str, Any]]:
    blocks = split_shipments(text)
    merged: Dict[str, Dict[str, Any]] = {}
    for block_idx, block in enumerate(blocks):
        header = extract_header(block)
        lines = extract_lines(block)
        for line_rec in lines:
            k = line_rec["k_number"]
            if k not in merged:
                record: Dict[str, Any] = {
                    "k_number":    k,
                    "raw_line":    line_rec["raw_line"],
                    "block_index": block_idx,
                    **header,
                    **{f: line_rec.get(f, "") for f, *_ in LINE_FIELDS},
                }
                merged[k] = record
            else:
                existing = merged[k]
                for field in list(header.keys()) + [f for f, *_ in LINE_FIELDS]:
                    new_val = line_rec.get(field) or header.get(field, "")
                    if new_val and not existing.get(field):
                        existing[field] = new_val
    return list(merged.values())

# ---------------------------------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def health():
    """Health check — Render gebruikt dit om te checken of de service leeft."""
    return jsonify({"status": "ok", "message": "Shipment parser API is running"})

@app.route("/parse", methods=["POST"])
def parse_endpoint():
    """
    Verwacht JSON body: { "text": "...bestandsinhoud..." }
    Geeft terug:        { "records": [...], "count": N }
    """
    data = request.get_json(silent=True)

    if not data or "text" not in data:
        return jsonify({"error": "Geef een JSON body mee met een 'text' veld"}), 400

    text = data["text"]
    if not isinstance(text, str) or not text.strip():
        return jsonify({"error": "'text' moet een niet-lege string zijn"}), 400

    try:
        records = parse(text)
        log.info("Parsed %d records from %d chars", len(records), len(text))
        return jsonify({"records": records, "count": len(records)})
    except Exception as e:
        log.error("Parse error: %s", e)
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
