import os

GCP_PROJECT = os.getenv("GCP_PROJECT", os.getenv("GOOGLE_CLOUD_PROJECT", "reunidas"))
BQ_DATASET = os.getenv("BQ_DATASET", "reunidas_api")
BQ_TABLE = os.getenv("BQ_TABLE", "escala_app")