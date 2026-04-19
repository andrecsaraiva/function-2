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

    def load_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            raise ValueError("A lista de linhas está vazia.")

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        load_job = self.client.load_table_from_json(
            rows,
            self.table_id,
            job_config=job_config
        )

        load_job.result()

        if load_job.errors:
            raise Exception(f"Erro ao carregar linhas no BigQuery: {load_job.errors}")

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
            "veic": "162203",
            "placa": "GDS 3I42",
            "atv": "ESCALA",
            "orig": "BAURU",
            "dest": "ARACATUBA",
            "obs": ""
        }

        return self.insert_row(row)

    def query(self, sql: str):
        query_job = self.client.query(sql)
        return query_job.result()
    
    def get_escala_by_cif(self, cif: str, view_name: str = "vw_escala_app_ordenada") -> List[Dict[str, Any]]:
        sql = f"""
            SELECT
              cif,
              snh,
              tip,
              saida,
              ds,
              hora_inicio,
              hora_final,
              chegada,
              veic,
              placa,
              atv,
              orig,
              dest,
              obs,
              cif_num,
              ordem_tipo,
              ordem_linha_cif
            FROM `{GCP_PROJECT}.{BQ_DATASET}.{view_name}`
            WHERE REGEXP_REPLACE(cif, r'^0+', '') = @cif
            ORDER BY ordem_linha_cif
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("cif", "STRING", cif)
            ]
        )

        query_job = self.client.query(sql, job_config=job_config)
        results = query_job.result()

        return [dict(row.items()) for row in results]