#!/usr/bin/env python3
"""Scraping NBB EN CONTINU — draine les hôtels « pending » jusqu'à épuisement.

Boucle sans fin utile : à chaque tour, prend un lot d'hôtels encore en statut
``pending`` (jamais tentés) dans ``scrape_state`` et télécharge leurs comptes
annuels NBB réels vers la couche Bronze locale + ``file_state``. S'arrête quand
il n'y a plus aucun hôtel ``pending``.

Conception anti-blocage : on ne sélectionne QUE les ``pending``. Un hôtel qui
échoue passe en ``in_progress`` (résumable) et n'est PAS re-pioché, donc la file
avance toujours — jamais coincé sur les mêmes échecs. Une passe de reprise
optionnelle (--retry) retente les ``in_progress`` à la fin.

Idempotent : un CSV déjà téléchargé est sauté (delta via State DB). On peut donc
tuer/relancer ce script à volonté, il reprend là où il s'était arrêté.

Env : BCE_MONGO_URI, BCE_HDFS_BACKEND=local, BCE_HDFS_BRONZE=<dossier>.
Usage :
  python3 scripts/scrape_loop.py                 # lot de 25, en continu
  SCRAPE_BATCH=50 python3 scripts/scrape_loop.py --retry
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bce_ingestion import config, mongo, scrape_nbb, state_db
from bce_ingestion.hdfs_io import get_hdfs


def _count(coll, status):
    return coll.count_documents({"status": status})


def _next_batch(coll, status, batch):
    return [d["_id"] for d in coll.find({"status": status}, {"_id": 1}).limit(batch)]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Scraping NBB en continu (drain des hôtels pending).")
    ap.add_argument("--batch", type=int, default=int(os.environ.get("SCRAPE_BATCH", "25")),
                    help="Nombre d'hôtels traités par tour (défaut 25).")
    ap.add_argument("--retry", action="store_true",
                    help="Après épuisement des pending, retenter les in_progress (échecs).")
    args = ap.parse_args(argv)

    db = mongo.get_db()
    hdfs = get_hdfs()
    coll = db[config.SCRAPE_STATE_COLLECTION]
    fstate = db[config.STATE_COLLECTION]

    total = coll.estimated_document_count()
    print(f"▶ scrape continu | backend={hdfs.backend} base={hdfs.base} | "
          f"hôtels ciblés={total} | batch={args.batch}", flush=True)

    def drain(status: str, label: str) -> None:
        rnd = 0
        scraped = 0
        while True:
            bces = _next_batch(coll, status, args.batch)
            if not bces:
                break
            rnd += 1
            docs_before = fstate.estimated_document_count()
            for bce in bces:
                try:
                    scrape_nbb.scrape_company_filings(db, hdfs, bce)
                except Exception:  # noqa: BLE001 — reste in_progress, non re-pioché
                    pass
                scraped += 1
            docs_after = fstate.estimated_document_count()
            done = _count(coll, "done")
            pending = _count(coll, "pending")
            inprog = _count(coll, "in_progress")
            print(f"[{label} r{rnd}] +{docs_after - docs_before} docs "
                  f"(total file_state={docs_after}) | "
                  f"done={done} pending={pending} in_progress={inprog}", flush=True)

    drain("pending", "pending")
    if args.retry:
        drain("in_progress", "retry")

    print(f"✓ scraping terminé — plus d'hôtels à traiter. "
          f"file_state={fstate.estimated_document_count()} "
          f"done={_count(coll, 'done')} in_progress={_count(coll, 'in_progress')}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
