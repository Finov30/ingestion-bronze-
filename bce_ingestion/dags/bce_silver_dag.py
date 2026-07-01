"""DAG Airflow — couche SILVER BCE (medallion Bronze -> Silver).

Construit ``config.SILVER_COLLECTION`` (``enterprise_silver``) à partir de la
Bronze « riche » ``config.FINALE_COLLECTION`` (``enterprise_finale``) via
``silver.build_silver`` : nettoyage, dénomination officielle, adresse du siège,
codes NACE dédupliqués, indicateur ``is_hospitality``.

Parse-time : AUCUNE connexion Mongo n'est ouverte au niveau module — tout se fait
DANS la tâche, de sorte que ``airflow dags list`` importe ce fichier sans MongoDB
vivant. La (re)construction de ``enterprise_finale`` est hors périmètre (on
suppose la Bronze présente).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from bce_ingestion import config, mongo, silver

log = logging.getLogger(__name__)


@dag(
    dag_id="bce_silver",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bce", "silver"],
)
def bce_silver():
    """Construit/rafraîchit la couche Silver depuis la Bronze enterprise_finale."""

    @task
    def build_silver_task() -> dict:
        """Ouvre Mongo DANS la tâche et délègue à silver.build_silver."""
        db = mongo.get_db()
        stats = silver.build_silver(db)
        log.info(
            "SILVER construite depuis %s -> %s : %s",
            config.FINALE_COLLECTION, config.SILVER_COLLECTION, stats,
        )
        return stats

    build_silver_task()


# --- Instanciation au niveau module (requise par Airflow) ------------------
silver_dag = bce_silver()
