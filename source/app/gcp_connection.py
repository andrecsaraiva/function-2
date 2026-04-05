from typing import List, Dict, Any
from google.cloud import bigquery

from app.data_config import GCP_PROJECT, BQ_DATASET, BQ_TABLE

class GcpConnection:
    def __init__(self):
        self.client = bigquery.Client(project=GCP_PROJECT)
        self.table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    def insert_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        errors = self.client.insert_rows_json(self.table_id, [row])

        if errors:
            raise Exception(f"Erro ao inserir linha no BigQuery: {errors}")

        return row

    def insert_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            raise ValueError("A lista de linhas está vazia.")

        errors = self.client.insert_rows_json(self.table_id, rows)

        if errors:
            raise Exception(f"Erro ao inserir linhas no BigQuery: {errors}")

        return rows

    def insert_test_row(self) -> Dict[str, Any]:
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

        return self.insert_row(row)

    def query(self, sql: str):
        query_job = self.client.query(sql)
        return query_job.result()