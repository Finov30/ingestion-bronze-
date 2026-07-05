"""Explorateur d'entreprises BCE — recherche + visualisation des documents liés.

Application web légère (Flask) au-dessus du pipeline d'ingestion :
- recherche d'entreprises par numéro BCE ou dénomination ;
- visualisation des documents Bronze liés (dépôts NBB/CBSO, statuts notaire,
  publications eJustice) et des KPI financiers des comptes annuels.

Source de données : MongoDB uniquement (voir data_source.py) — aucune donnée
synthétique. Si la base est injoignable, une erreur explicite est renvoyée
plutôt que de fausses fiches.
"""
