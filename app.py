"""
app.py - Shipment parser API voor Render.com
Gebaseerd op het echte bestandsformaat van de klant.

Formaat per shipment (alles plat, spatie-gescheiden):
SEA {job_nr} {vessel} {voyage} {bl_number} {pol} {city_pod} {etd} {eta} [atd]
{shipper} {fcl/lcl} {container} {container_type} {bl_ref} {weight} {cbm} {qty}
[K-nummers lijst] [regelitems: {seq} {k_nr} {omschrijving} {qty} CARTONS/PALLETS {weight} {cbm}]
"""

import re
import logging
from typing import Any, Dict, List, Optional
from flask import Flask, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

K_PATTERN = re.compile(r'\b(K(?:M?\d{2,4}|\d{2,4})-\d{3,6})\b', re.IGNORECASE)
DATE_PATTERN = re.compile(r'\b(\d{2}/\d{2}/\d{4})\b')
CONTAINER_PATTERN = re.compile(r'\b([A-Z]{4}\d{7})\b')
UNIT_PATTERN = re.compile(r'\b(CARTONS?|PALLETS?|PACKAGES?|PIECES?|PCS|CTNS?)\b', re.IGNORECASE)

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def normalise_k(k: str) -> str:
    return k.upper().strip()

def find_all_k(text: str) -> List[str]:
    return [normalise_k(m) for m in K_PATTERN.findall(text)]

def find_dates(text: str) -> List[str]:
    return DATE_PATTERN.findall(text)

def find_containers(text: str) -> List[str]:
    return CONTAINER_PATTERN.findall(text)

# ---------------------------------------------------------------------------
# SPLIT INTO SHIPMENT BLOCKS
# ---------------------------------------------------------------------------

def split_shipments(text: str) -> List[str]:
    """Split de platte tekst op elke 'SEA {nummer}' marker."""
    parts = re.split(r'(?=\bSEA\s+\d{8}\b)', text.strip())
    valid = [p.strip() for p in parts if p.strip() and K_PATTERN.search(p)]
    log.info("split_shipments: %d blokken gevonden", len(valid))
    return valid

# ---------------------------------------------------------------------------
# PARSE SHIPMENT HEADER
# ---------------------------------------------------------------------------

def extract_header(block: str) -> Dict[str, str]:
    """
    Haalt shipment-level info uit een blok.
    Volgorde in het bestand: SEA jobnr vessel voyage bl_nr pol pod etd [eta] [atd]
    """
    tokens = block.split()
    header: Dict[str, str] = {
        "job_number": "",
        "vessel": "",
        "voyage": "",
        "bl_number": "",
        "pol": "",
        "pod": "Rotterdam",  # altijd Rotterdam in dit bestand
        "etd": "",
        "eta": "",
        "atd": "",
        "shipper": "",
        "transport_type": "",  # FCL / LCL
        "container": "",
        "container_type": "",
        "total_weight": "",
        "total_cbm": "",
        "total_pieces": "",
    }

    # Job number: eerste getal na SEA
    m = re.match(r'SEA\s+(\d{8})', block)
    if m:
        header["job_number"] = m.group(1)

    # Alle datums in het blok
    dates = find_dates(block)
    if len(dates) >= 1:
        header["etd"] = dates[0]
    if len(dates) >= 2:
        header["eta"] = dates[1]
    if len(dates) >= 3:
        header["atd"] = dates[2]

    # Containers
    containers = find_containers(block)
    if containers:
        header["container"] = containers[0]

    # FCL / LCL
    fcl_lcl = re.search(r'\b(FCL|LCL)\b', block)
    if fcl_lcl:
        header["transport_type"] = fcl_lcl.group(1)

    # Container type (20STD, 40HC, 40STD etc.)
    ct = re.search(r'\b(20STD|40STD|40HC|20HC|45HC)\b', block)
    if ct:
        header["container_type"] = ct.group(1)

    # Vessel + voyage: na het job_number komen vessel naam en voyage
    # Patroon: na job_nr een reeks hoofdletters/cijfers/spaties tot aan een BL-nummer
    # BL-nummer herkennen: begint met 2 letters + cijfers of TJRTM/NBRTM etc.
    vessel_block = re.search(
        r'SEA\s+\d{8}\s+(.*?)\s+([A-Z]{2,6}\d{2,4}[A-Z0-9]*)\s+',
        block
    )
    if vessel_block:
        header["vessel"] = vessel_block.group(1).strip()
        header["voyage"] = vessel_block.group(2).strip()

    # POL: stad voor Rotterdam (laatste woord voor Rotterdam)
    pol_match = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+Rotterdam', block)
    if pol_match:
        header["pol"] = pol_match.group(1).strip()

    # Shipper: naam voor FCL/LCL (voor het transport type keyword)
    shipper_match = re.search(r'Rotterdam\s+(?:\d{2}/\d{2}/\d{4}\s+)*(.+?)\s+(?:FCL|LCL)\b', block)
    if shipper_match:
        header["shipper"] = shipper_match.group(1).strip()

    # Totalen: drie getallen vlak voor de K-nummers lijst
    # Patroon: getal getal getal gevolgd door K-nummer
    totals_match = re.search(
        r'(\d+(?:\.\d+)?)\s+(\d+(?:\.\d+)?)\s+(\d+)\s+(?:K(?:M?\d|24|25))',
        block
    )
    if totals_match:
        header["total_weight"] = totals_match.group(1)
        header["total_cbm"] = totals_match.group(2)
        header["total_pieces"] = totals_match.group(3)

    return header

# ---------------------------------------------------------------------------
# PARSE ORDER LINES
# ---------------------------------------------------------------------------

def extract_lines(block: str) -> List[Dict[str, Any]]:
    """
    Haalt individuele K-nummer orderregels uit een blok.
    Patroon per regel: {seq_nr} {K-nummer} {omschrijving?} {qty} CARTONS/PALLETS {weight} {cbm}
    """
    results: List[Dict[str, Any]] = []

    # Zoek alle occurrences van: getal K-nummer ... qty UNIT weight cbm
    line_pattern = re.compile(
        r'(\d{1,2})\s+'                          # seq nummer
        r'(K(?:M?\d{2,4}|\d{2,4})-\d{3,6})\s+'  # K-nummer
        r'(.*?)'                                   # optionele omschrijving
        r'(\d[\d,\.]*)\s+'                         # quantity
        r'(CARTONS?|PALLETS?|PACKAGES?|PIECES?|PCS|CTNS?)\s+'  # unit
        r'(\d[\d,\.]*)\s+'                         # weight
        r'(\d[\d,\.]*)',                           # cbm
        re.IGNORECASE
    )

    for m in line_pattern.finditer(block):
        seq         = m.group(1)
        k_raw       = m.group(2)
        description = m.group(3).strip()
        quantity    = m.group(4)
        unit        = m.group(5).upper()
        weight      = m.group(6)
        cbm         = m.group(7)

        results.append({
            "k_number":    normalise_k(k_raw),
            "seq":         seq,
            "description": description,
            "quantity":    quantity,
            "unit":        unit,
            "weight_kg":   weight,
            "cbm":         cbm,
            "raw_line":    m.group(0).strip(),
        })

    return results

# ---------------------------------------------------------------------------
# MAIN PARSE
# ---------------------------------------------------------------------------

def parse(text: str) -> List[Dict[str, Any]]:
    """
    Hoofdfunctie: geeft één record per unieke K-nummer + seq combinatie.
    K-nummers die meerdere keren voorkomen (zelfde K, verschillende seq)
    krijgen elk hun eigen record met de shipment-header erbij.
    """
    blocks = split_shipments(text)
    results: List[Dict[str, Any]] = []
    seen_keys = set()

    for block in blocks:
        header = extract_header(block)
        lines  = extract_lines(block)

        for line in lines:
            # Dedup key = k_number + seq + job_number
            dedup_key = f"{line['k_number']}_{line['seq']}_{header['job_number']}"
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

            record = {**header, **line}
            results.append(record)

    log.info("parse: %d records gevonden", len(results))
    return results

# ---------------------------------------------------------------------------
# API ENDPOINTS
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "message": "Shipment parser API draait"})

@app.route("/parse", methods=["POST"])
def parse_endpoint():
    data = request.get_json(silent=True)
    if not data or "text" not in data:
        return jsonify({"error": "Geef een JSON body met een 'text' veld"}), 400

    text = data["text"]
    if not isinstance(text, str) or not text.strip():
        return jsonify({"error": "'text' moet een niet-lege string zijn"}), 400

    try:
        records = parse(text)
        return jsonify({"records": records, "count": len(records)})
    except Exception as e:
        log.error("Parse fout: %s", e)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
