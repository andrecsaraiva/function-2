import csv
import io
import logging
from flask import jsonify

from app.gcp_connection import GcpConnection
from app.obter_arquivo import obter_arquivo_request


def _montar_linha_escala(row: list[str]) -> dict:
    """
    Mapeia uma linha do CSV para o schema atual da tabela BigQuery.

    Estrutura correta esperada:
    cif;snh;tip;saida;ds;inicio;final;chegada;veic;placa;atv;orig;dest;obs
    """
    if len(row) < 14:
        row = row + [""] * (14 - len(row))
    elif len(row) > 14:
        row = row[:13] + [";".join(row[13:])]

    return {
        "cif": row[0].strip(),
        "snh": row[1].strip(),
        "tip": row[2].strip(),
        "saida": row[3].strip(),
        "ds": row[4].strip(),
        "hora_inicio": row[5].strip(),
        "hora_final": row[6].strip(),
        "chegada": row[7].strip(),
        "veic": row[8].strip(),
        "placa": row[9].strip(),
        "atv": row[10].strip(),
        "orig": row[11].strip(),
        "dest": row[12].strip(),
        "obs": row[13].strip(),
    }


def _ler_csv_escala(arquivo) -> list[dict]:
    arquivo.stream.seek(0)
    wrapper = io.TextIOWrapper(
        arquivo.stream,
        encoding="utf-8",
        errors="replace",
        newline=""
    )

    try:
        reader = csv.reader(wrapper, delimiter=";")
        header = next(reader, None)

        if not header:
            raise ValueError("O arquivo CSV está vazio.")

        logging.info(f"Cabeçalho CSV recebido: {header}")

        rows = []
        for line_number, row in enumerate(reader, start=2):
            if not row or all(not str(col).strip() for col in row):
                continue

            try:
                rows.append(_montar_linha_escala(row))
            except Exception as e:
                raise ValueError(f"Erro ao tratar a linha {line_number} do CSV: {e}")

        if not rows:
            raise ValueError("O CSV não possui linhas de dados válidas.")

        return rows

    finally:
        try:
            wrapper.detach()
        except Exception:
            pass
        arquivo.stream.seek(0)


def run(validated_request, request_id: str):
    logging.info(f"[{request_id}] Tipo do request: {validated_request.request_type}")
    logging.info(f"[{request_id}] Action recebida: {validated_request.action}")

    arquivo_info = None
    conn = GcpConnection()

    if validated_request.request_type == "form-data":
        arquivo_info = obter_arquivo_request(validated_request.arquivo)
        logging.info(f"[{request_id}] Arquivo recebido via form-data: {arquivo_info}")

        if validated_request.action == "importar_escala_csv":
            rows = _ler_csv_escala(validated_request.arquivo)
            conn.load_rows(rows)

            return jsonify({
                "success": True,
                "request_id": request_id,
                "message": "CSV importado e carregado com sucesso na BigQuery.",
                "request_type": validated_request.request_type,
                "action": validated_request.action,
                "arquivo_info": arquivo_info,
                "table": conn.table_id,
                "inserted_count": len(rows),
                "sample_rows": rows[:5]
            }), 200

    elif validated_request.request_type == "json":
        if validated_request.arquivo_path:
            logging.info(
                f"[{request_id}] Caminho de arquivo recebido via JSON: {validated_request.arquivo_path}"
            )

            # TODO:
            # Aqui futuramente vamos buscar o arquivo no bucket
            # usando o caminho/link recebido em 'arquivo'.

    inserted_row = conn.insert_test_row()

    return jsonify({
        "success": True,
        "request_id": request_id,
        "message": "Requisição validada e linha de teste inserida com sucesso no BigQuery.",
        "request_type": validated_request.request_type,
        "action": validated_request.action,
        "arquivo_info": arquivo_info,
        "arquivo_json": validated_request.arquivo_path,
        "table": conn.table_id,
        "inserted_row": inserted_row
    }), 200