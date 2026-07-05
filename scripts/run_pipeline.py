#!/usr/bin/env python3
"""Orchestrateur du pipeline de peuplement BCE — enchaîne les VRAIS flows du projet.

Exécute, dans l'ordre médaillon, les flows qui peuplent MongoDB à partir du dump
KBO Open Data (aucune donnée inventée) :

  [1] seed_companies.seed            -> companies
  [2] build_bronze.build_...finale   -> enterprise_finale
  [3] silver.build_silver            -> enterprise_silver
  [4] hotel.load_hotels_to_state     -> scrape_state (hôtels ciblés « pending »)
  [5] scrape_nbb.scrape_pending_hotels -> file_state + Bronze (comptes NBB réels)

Pré-requis : le dump KBO décompressé dans un dossier (enterprise.csv,
denomination.csv, address.csv, activity.csv, code.csv, et optionnellement
contact/establishment/branch.csv), et MongoDB lancé.

Exemples :
  export BCE_MONGO_URI=mongodb://localhost:27017 BCE_MONGO_DB=bce
  export BCE_HDFS_BACKEND=local BCE_HDFS_BRONZE=/home/hdcc5629/ingestion-bronze/bronze_local
  # test rapide sur 50 000 entreprises, puis scraping de 30 hôtels :
  python3 scripts/run_pipeline.py --kbo-dir /home/hdcc5629/kbo --limit 50000 --scrape-limit 30
  # registre complet (long) :
  python3 scripts/run_pipeline.py --kbo-dir /home/hdcc5629/kbo --scrape-limit 200
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bce_ingestion import (build_bronze, config, hotel, scrape_nbb,
                           seed_companies, silver, state_db)
from bce_ingestion.hdfs_io import get_hdfs
from bce_ingestion.seed_companies import bce10

REQUIRED = ["enterprise.csv", "denomination.csv", "address.csv", "activity.csv", "code.csv"]
OPTIONAL = ["contact.csv", "establishment.csv", "branch.csv"]


def _scan_hotel_bces(activity_csv: str, prefixes: list[str], chunksize: int = 200000) -> set[str]:
    """Pré-sélectionne les BCE dont l'activité PRINCIPALE relève de l'hébergement.

    Streame activity.csv (34 M lignes) en gardant seulement les EntityNumber dont
    ``Classification == 'MAIN'`` et ``NaceCode`` commence par un préfixe hôtelier
    (défaut '55'). Mémoire bornée : on ne retient qu'un set de numéros. Ce set sert
    d'argument ``bces`` à build_bronze → on ne construit le Bronze riche QUE pour
    les hôtels (le registre complet reste, lui, dans ``companies`` via le seed).
    """
    import pandas as pd
    pref = tuple(prefixes)
    hotels: set[str] = set()
    for chunk in pd.read_csv(activity_csv, dtype=str, chunksize=chunksize,
                             keep_default_na=False, na_values=[]):
        sub = chunk[(chunk["Classification"] == "MAIN")
                    & (chunk["NaceCode"].str.startswith(pref))]
        for num in sub["EntityNumber"]:
            b = bce10(num)
            if b:
                hotels.add(b)
    return hotels


def _check_kbo(kbo_dir: str) -> list[str]:
    missing = [f for f in REQUIRED if not os.path.isfile(os.path.join(kbo_dir, f))]
    return missing


def _counts(db) -> str:
    def n(c):
        return db[c].estimated_document_count()
    return (f"companies={n(config.COMPANIES_COLLECTION)} "
            f"finale={n(config.FINALE_COLLECTION)} "
            f"silver={n(config.SILVER_COLLECTION)} "
            f"scrape_state={n(config.SCRAPE_STATE_COLLECTION)} "
            f"file_state={n(config.STATE_COLLECTION)}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Enchaîne les flows de peuplement BCE (données réelles).")
    ap.add_argument("--kbo-dir", required=True, help="Dossier des CSV KBO décompressés.")
    ap.add_argument("--limit", type=int, default=None,
                    help="Nombre max d'entreprises pour seed/bronze (None = tout le registre).")
    ap.add_argument("--hotels-only", action="store_true",
                    help="Seed complet du registre, puis build Bronze/Silver UNIQUEMENT "
                         "pour les hôtels réels (NACE 55 MAIN). Recommandé : capture TOUS "
                         "les hôtels sans charger 34 M de lignes d'activité en mémoire.")
    ap.add_argument("--scrape-limit", type=int, default=30,
                    help="Nombre d'hôtels à scraper au flow 5 (résumable, défaut 30).")
    ap.add_argument("--skip-scrape", action="store_true",
                    help="S'arrêter après le ciblage (ne pas scraper les comptes NBB).")
    args = ap.parse_args(argv)

    kbo = os.path.abspath(args.kbo_dir)
    missing = _check_kbo(kbo)
    if missing:
        print(f"❌ CSV KBO manquants dans {kbo} : {', '.join(missing)}")
        print("   Téléchargez le dump KBO Open Data (compte gratuit "
              "kbopub.economie.fgov.be/kbo-open-data), décompressez-le ici, puis relancez.")
        return 2

    code_csv = os.path.join(kbo, "code.csv")
    enterprise_csv = os.path.join(kbo, "enterprise.csv")
    denomination_csv = os.path.join(kbo, "denomination.csv")

    from bce_ingestion import mongo
    db = mongo.get_db()
    state_db.ensure_indexes(db)
    state_db.ensure_scrape_indexes(db)
    hdfs = get_hdfs()

    print(f"▶ Pipeline BCE | KBO={kbo} | limit={args.limit} | backend HDFS={hdfs.backend}")
    print(f"  état initial : {_counts(db)}\n")

    # [1] seed_companies -> companies (registre complet si --hotels-only).
    #     En mode hotels-only on n'ajoute pas les dénominations ici : elles seront
    #     portées par le build Silver ciblé (évite de charger 1,95 M noms en RAM).
    seed_limit = None if args.hotels_only else args.limit
    print(f"[1/5] seed_companies -> companies (limit={seed_limit}) …")
    r1 = seed_companies.seed(db, csv_path=enterprise_csv, limit=seed_limit,
                             with_denomination=not args.hotels_only,
                             denomination_csv=denomination_csv)
    print(f"      {r1}")

    # [2] build_bronze -> enterprise_finale
    if args.hotels_only:
        prefixes = config.HOTEL_NACE_PREFIXES or ["55"]
        print(f"[2/5] pré-sélection des hôtels (NACE MAIN {prefixes}) dans activity.csv …")
        hotel_bces = _scan_hotel_bces(os.path.join(kbo, "activity.csv"), prefixes)
        print(f"      {len(hotel_bces)} entités hôtelières détectées → build Bronze ciblé")
        r2 = build_bronze.build_enterprise_finale(db, bces=hotel_bces, kbo_dir=kbo)
    else:
        print(f"[2/5] build_bronze -> enterprise_finale (limit={args.limit}) …")
        r2 = build_bronze.build_enterprise_finale(db, kbo_dir=kbo, limit=args.limit)
    print(f"      companies={r2['companies']} activities={r2['activities']} "
          f"addresses={r2['addresses']} denominations={r2['denominations']}")

    # [3] silver -> enterprise_silver
    print("[3/5] silver -> enterprise_silver …")
    r3 = silver.build_silver(db, code_csv=code_csv)
    print(f"      {r3}")

    # [4] hotel -> scrape_state (ciblage NACE 55)
    print("[4/5] hotel (ciblage NACE 55) -> scrape_state …")
    r4 = hotel.load_hotels_to_state(db, collection=config.SILVER_COLLECTION)
    hotels = db[config.SCRAPE_STATE_COLLECTION].count_documents({})
    print(f"      hôtels ciblés (pending posés ce run) : {r4['loaded']} | total scrape_state : {hotels}")

    # [5] scrape_nbb -> file_state + Bronze (comptes annuels réels)
    if args.skip_scrape:
        print("[5/5] scraping NBB : SAUTÉ (--skip-scrape)")
    else:
        print(f"[5/5] scrape_nbb -> comptes NBB réels (limit={args.scrape_limit}) …")
        r5 = scrape_nbb.scrape_pending_hotels(db, hdfs, limit=args.scrape_limit)
        print(f"      {r5}")

    print(f"\n✓ terminé. état final : {_counts(db)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
