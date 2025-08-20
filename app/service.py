import csv
from io import StringIO
from fastapi import UploadFile
from collections import Counter

def normalize_value(value: str) -> str:
    v = value.replace("R$", "").replace(",", ".").strip()
    v = v.replace(".", "", v.count(".") - 1)
    f = float(v)
    return f"-{f:.2f}" if f >= 0 else None

def load_transactions(file: UploadFile) -> list[dict]:
    content = file.file.read().decode("utf-8-sig")
    file.file.seek(0)
    reader = csv.DictReader(StringIO(content), delimiter=";")
    result = []
    for row in reader:
        date = row.get("Data", "").strip()
        desc = row.get("Estabelecimento", "").strip()
        val = normalize_value(row.get("Valor", ""))
        if val is None:
            continue
        result.append({"date": date, "description": desc, "value": val})
    return result

def filter_transactions(invoice_file: UploadFile, computed_file: UploadFile | None):
    invoice_tx = load_transactions(invoice_file)

    if not computed_file:
        return invoice_tx

    computed_tx = load_transactions(computed_file)
    computed_counts = Counter((t["description"], t["value"]) for t in computed_tx)
    temp_counts = Counter()

    filtered = []
    for tx in sorted(invoice_tx, key=lambda x: x["date"]):
        key = (tx["description"], tx["value"])
        if temp_counts[key] < computed_counts[key]:
            temp_counts[key] += 1
            continue
        filtered.append(tx)

    return filtered

def generate_csv(transactions: list[dict]) -> StringIO:
    output = StringIO()
    writer = csv.writer(output, delimiter=";", quoting=csv.QUOTE_ALL)
    writer.writerow(["Data", "Descrição", "Valor", "Conta", "Categoria"])
    for tx in transactions:
        writer.writerow([tx["date"], tx["description"], tx["value"], "Carteira", "Alimentação"])
    output.seek(0)
    return output
