"""Job Spark — construction de la couche GOLD ``hotel_gold`` à l'échelle.

Ce driver lit la couche Bronze (``config.HDFS_BRONZE``) où les dépôts BNB sont
stockés sous ``{bce}/nbb/{year}/*.csv`` (l'énoncé écrit parfois ``{bce}/hbb/`` —
c'est le même dossier ``nbb``), calcule un document GOLD par entreprise et
l'upserte dans Mongo (``config.GOLD_COLLECTION``).

Toute la LOGIQUE FINANCIÈRE est déléguée à ``gold.py`` (parse_pcmn, gold_fields,
compute_ratios, build_year_entry, build_gold_for_company) : Spark ne fait que la
distribution des fichiers et l'écriture. ``gold.py`` reste ainsi l'unique source
de vérité, testable sans Spark.

Prérequis d'EXÉCUTION (pas d'import) : un JDK (Java) pour Spark, un cluster/HDFS
accessible et un MongoDB joignable (``config.MONGO_URI``). Importer ce module ne
démarre AUCUN contexte Spark : tout se trouve dans ``run_gold_spark`` / ``main``.

Chemin de données Spark
-----------------------
1. ``sparkContext.wholeTextFiles("{HDFS_BRONZE}/*/nbb/**")`` -> RDD ``(path, text)`` ;
2. ``map`` -> ``(bce, csv_text)`` (bce extrait du chemin) ;
3. ``groupByKey`` -> ``(bce, [csv_text, ...])`` ;
4. ``map`` -> document GOLD via ``gold.build_gold_for_company`` ;
5. ``foreachPartition`` -> upsert Mongo avec pymongo (une connexion par partition).
"""
from __future__ import annotations

import re

from . import config
from . import gold


# ---------------------------------------------------------------------------
# Extraction du numéro BCE depuis un chemin HDFS
# ---------------------------------------------------------------------------
def bce_from_path(path: str) -> str | None:
    """Extrait le numéro d'entreprise d'un chemin ``.../{bce}/nbb/{year}/....csv``.

    On repère le segment situé juste avant ``nbb`` (ou ``hbb``). À défaut, on
    retient le plus long segment purement numérique du chemin.
    """
    parts = [p for p in re.split(r"[\\/]+", path) if p]
    for i, seg in enumerate(parts):
        if seg.lower() in ("nbb", "hbb") and i > 0:
            return parts[i - 1]
    numeric = [p for p in parts if p.isdigit()]
    return max(numeric, key=len) if numeric else None


# ---------------------------------------------------------------------------
# Écriture Mongo par partition (une connexion par exécuteur)
# ---------------------------------------------------------------------------
def _upsert_partition(docs_iter):
    """Upsert d'une partition de documents GOLD dans Mongo (côté exécuteur)."""
    from pymongo import MongoClient, ReplaceOne

    client = MongoClient(config.MONGO_URI)
    try:
        coll = client[config.MONGO_DB][config.GOLD_COLLECTION]
        ops = [
            ReplaceOne({"enterprise_number": d["enterprise_number"]}, d, upsert=True)
            for d in docs_iter
            if d and d.get("years")
        ]
        if ops:
            coll.bulk_write(ops, ordered=False)
    finally:
        client.close()
    return iter(())


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def run_gold_spark(bronze_base: str | None = None, spark=None, write_mongo: bool = True) -> int:
    """Exécute le job Spark GOLD et renvoie le nombre d'entreprises traitées.

    Args:
        bronze_base : racine Bronze (défaut ``config.HDFS_BRONZE``).
        spark       : ``SparkSession`` existante (sinon une est créée puis arrêtée).
        write_mongo : si ``False``, on calcule sans écrire (utile en dry-run).
    """
    from pyspark.sql import SparkSession

    base = (bronze_base or config.HDFS_BRONZE).rstrip("/")
    owns_spark = spark is None
    if spark is None:
        spark = SparkSession.builder.appName("bce-gold").getOrCreate()

    try:
        sc = spark.sparkContext
        # Tous les CSV sous {bce}/nbb/... (récursif). wholeTextFiles -> (path, contenu).
        pattern = f"{base}/*/nbb/*/*.csv"
        raw = sc.wholeTextFiles(pattern)

        pairs = (
            raw.map(lambda kv: (bce_from_path(kv[0]), kv[1]))
            .filter(lambda kv: kv[0] is not None)
        )
        grouped = pairs.groupByKey()
        docs = grouped.map(
            lambda kv: gold.build_gold_for_company(kv[0], list(kv[1]))
        ).filter(lambda d: bool(d.get("years")))

        if write_mongo:
            count = docs.count()
            docs.foreachPartition(_upsert_partition)
        else:
            count = docs.count()
        return count
    finally:
        if owns_spark:
            spark.stop()


def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Job Spark GOLD (hotel_gold).")
    parser.add_argument("--bronze-base", default=config.HDFS_BRONZE)
    parser.add_argument("--dry-run", action="store_true", help="Ne pas écrire dans Mongo.")
    args = parser.parse_args(argv)

    n = run_gold_spark(bronze_base=args.bronze_base, write_mongo=not args.dry_run)
    print(f"GOLD Spark : {n} entreprise(s) traitée(s).")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
