from google.cloud import bigquery
from app.data_config import GCP_PROJECT, BQ_DATASET, BQ_TABLE

class GcpConnection:
    def __init__(self):
        self.client = bigquery.Client(project=GCP_PROJECT)
        self.table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    def insert_test_row(self) -> dict:
        row = {
            "cif": "009001",
            "snh": "123456",
            "tip": "A",
            "saida": "07/02/2026",
            "ds": "Sab",
            "hora_inicio": "03:30",
            "hora_final": "06:10",
            "chegada": "07/02/2026",
            "veic_placa": "162203 GDS 3I42",
            "atv": "ESCALA",
            "orig": "BAURU",
            "dest": "ARACATUBA",
            "obs": "",
            "motorista": ""
        }

        errors = self.client.insert_rows_json(self.table_id, [row])

        if errors:
            raise Exception(f"Erro ao inserir no BigQuery: {errors}")

        return row