"""Explorateur d'entreprises BCE — recherche + visualisation des documents liés.

Application web légère (Flask) au-dessus du pipeline d'ingestion :
- recherche d'entreprises par numéro BCE ou dénomination ;
- visualisation des documents Bronze liés (dépôts NBB/CBSO, statuts notaire,
  publications eJustice) et des KPI financiers des comptes annuels.

Source de données hybride (voir data_source.py) : interroge MongoDB si disponible,
sinon bascule sur un jeu de données représentatif déterministe (sample_data.py).
"""
