"""
app.py - Shipment parser API voor Render.com
Parseert de echte XML van Ritra Cargo naar nette records voor monday.com.

Endpoint: POST /parse
Body (JSON): { "text": "...xml bestandsinhoud als string..." }
Response:    { "records": [...], "count": N }
"""

import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, List
from flask import Flask, jsonify, request

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def _txt(element, tag: str, default: str = "") -> str:
    node = element.find(tag)
    if node is None or node.text is None:
        return default
    return node.text.strip()

def parse(text: str) -> List[Dict[str, Any]]:
    # Verwijder XML declaratie zodat ET niet klaagt over encoding
    clean = text
    if text.strip().startswith("<?xml"):
        end = text.find("?>")
        if end != -1:
            clean = text[end + 2:].strip()

    try:
        root = ET.fromstring(clean)
    except ET.ParseError as e:
        log.error("XML parse fout: %s", e)
        raise ValueError(f"Ongeldige XML: {e}")

    shipments = root.findall(".//ShipmentDetails")
    log.info("Gevonden shipments: %d", len(shipments))

    records: List[Dict[str, Any]] = []

    for shipment in shipments:

        # Vessel info
        v = shipment.find("Vessel")
        vessel_data = {
            "transport_type":    _txt(v, "Type"),
            "job_number":        _txt(v, "RitraRef"),
            "vessel":            _txt(v, "ModalityName"),
            "voyage":            _txt(v, "Voyage"),
            "carrier":           _txt(v, "Carrier"),
            "bl_number":         _txt(v, "BLnumber"),
            "pol":               _txt(v, "PortOfLoading"),
            "pod":               _txt(v, "PortOfDischarge"),
            "etd":               _txt(v, "ETD"),
            "eta":               _txt(v, "ETA"),
            "ata":               _txt(v, "ATA"),
            "initial_eta":       _txt(v, "Initial_ETA"),
            "initial_leadtime":  _txt(v, "Initial_Leadtime"),
            "real_leadtime":     _txt(v, "Real_Leadtime"),
        } if v is not None else {}

        # Shipment info
        shipper = _txt(shipment, "Shippername")
        status  = _txt(shipment, "Status")

        # Container (eerste)
        c = shipment.find("Container")
        container_data = {
            "modality":       _txt(c, "Modality"),
            "container":      _txt(c, "Container"),
            "container_type": _txt(c, "ContType"),
            "seal_number":    _txt(c, "Sealnr"),
            "stripping_date": _txt(c, "StrippingDate"),
            "total_weight":   _txt(c, "Weight"),
            "total_cbm":      _txt(c, "CBM"),
            "total_colli":    _txt(c, "Colli"),
            "deliver_name":   _txt(c, "DeliverName"),
            "deliver_street": _txt(c, "DeliverAddress"),
            "deliver_zip":    _txt(c, "DeliverZip"),
            "deliver_city":   _txt(c, "DeliverCity"),
            "deliver_country":_txt(c, "DeliverCountry"),
            "deliver_date":   _txt(c, "DeliverDate"),
        } if c is not None else {}

        base = {
            **vessel_data,
            "shipper": shipper,
            "status":  status,
            **container_data,
        }

        # Booking lines
        for line in shipment.findall(".//BookingLines/Line"):
            k = _txt(line, "POnumber")
            if not k:
                continue
            records.append({
                **base,
                "k_number":     k.upper().strip(),
                "line_number":  _txt(line, "No"),
                "order_number": _txt(line, "OrderNo"),
                "color_size":   _txt(line, "ColorSize"),
                "other_refs":   _txt(line, "OtherRefs"),
                "quantity":     _txt(line, "Colli"),
                "unit":         _txt(line, "Package"),
                "weight_kg":    _txt(line, "Weight"),
                "cbm":          _txt(line, "CBM"),
                "description":  _txt(line, "Description"),
            })

    log.info("parse: %d records gevonden", len(records))
    return records

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
