#!/usr/bin/env python3
"""Backfill des KPIs sur les documents NBB déjà ingérés SANS indicateurs.

Les dépôts NBB téléchargés avant l'ajout du calcul de KPIs (ou par un flux qui ne
les calculait pas) ont un ``file_state.financials`` absent/null. Ce script relit
leur CSV Bronze LOCAL, recalcule les KPIs (``kpi.financials_from_codes``) et met à
jour le document — sans re-télécharger quoi que ce soit.

Env : BCE_MONGO_URI, BCE_MONGO_DB.
Usage : python3 scripts/backfill_kpi.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bce_ingestion import config, mongo
from bce_ingestion.kpi import financials_from_codes
from bce_ingestion.sources import consult_nbb


def main() -> int:
    db = mongo.get_db()
    fs = db[config.STATE_COLLECTION]
    # financials null OU absent : {financials: None} matche les deux dans Mongo.
    q = {"source": "nbb", "kind": "csv", "status": "done", "financials": None}
    todo = fs.count_documents(q)
    print(f"Documents NBB sans KPI à backfiller : {todo}", flush=True)

    updated = missing = errors = 0
    for doc in fs.find(q):
        path = doc.get("hdfs_path")
        if not path or not os.path.isfile(path):
            missing += 1
            continue
        try:
            with open(path, encoding="utf-8") as fh:
                text = fh.read()
            fin = financials_from_codes(consult_nbb.parse_csv(text), doc.get("year"))
            fs.update_one({"_id": doc["_id"]}, {"$set": {"financials": fin}})
            updated += 1
        except Exception as exc:  # noqa: BLE001
            errors += 1
            if errors <= 5:
                print(f"  err {doc.get('bce')}/{doc.get('ref')}: {str(exc)[:80]}")
        if updated and updated % 100 == 0:
            print(f"  … {updated} mis à jour", flush=True)

    print(f"✓ backfill terminé : {updated} mis à jour, "
          f"{missing} fichiers absents, {errors} erreurs", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
