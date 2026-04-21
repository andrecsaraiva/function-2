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

def _decodificar_csv_bytes(raw_bytes: bytes) -> str:
    encodings = ["utf-8-sig", "cp1252", "latin1"]

    for encoding in encodings:
        try:
            texto = raw_bytes.decode(encoding)
            logging.info(f"CSV decodificado com sucesso usando {encoding}")
            return texto
        except UnicodeDecodeError:
            continue

    texto = raw_bytes.decode("latin1", errors="replace")
    logging.warning("CSV decodificado com fallback latin1 + replace")
    return texto

def _ler_csv_escala(arquivo) -> list[dict]:
    arquivo.stream.seek(0)
    raw_bytes = arquivo.stream.read()
    arquivo.stream.seek(0)

    if not raw_bytes:
        raise ValueError("O arquivo CSV está vazio.")

    csv_text = _decodificar_csv_bytes(raw_bytes)

    reader = csv.reader(io.StringIO(csv_text), delimiter=";")
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

def _normalizar_cif(cif: str) -> str:
    cif = str(cif or "").strip()
    if not cif:
        return ""
    return cif.lstrip("0") or "0"

def _montar_resposta_escala(rows: list[dict]) -> dict:
    identificacao = next((row for row in rows if row.get("tip") == "0"), None)
    avisos = [row for row in rows if row.get("tip") not in ("0", "A", "E")]
    escalas = [row for row in rows if row.get("tip") in ("A", "E")]

    return {
        "identificacao": identificacao,
        "avisos": avisos,
        "escalas": escalas,
        "rows": rows
    }

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
        if validated_request.action == "buscar_escala_por_cif":
            cif_informado = str(validated_request.data.get("cif", "")).strip()
            if not cif_informado:
                raise ValueError("Para a action 'buscar_escala_por_cif', o parâmetro 'cif' é obrigatório.")

            cif_consultado = _normalizar_cif(cif_informado)
            rows = conn.get_escala_by_cif(cif_consultado)

            if not rows:
                return jsonify({
                    "success": False,
                    "request_id": request_id,
                    "action": validated_request.action,
                    "cif_informado": cif_informado,
                    "cif_consultado": cif_consultado,
                    "message": "Nenhuma escala encontrada para o CIF informado."
                }), 404

            resposta = _montar_resposta_escala(rows)

            return jsonify({
                "success": True,
                "request_id": request_id,
                "action": validated_request.action,
                "cif_informado": cif_informado,
                "cif_consultado": cif_consultado,
                "total_rows": len(rows),
                "identificacao": resposta["identificacao"],
                "avisos": resposta["avisos"],
                "escalas": resposta["escalas"],
                "rows": resposta["rows"]
            }), 200

        if validated_request.arquivo_path:
            logging.info(
                f"[{request_id}] Caminho de arquivo recebido via JSON: {validated_request.arquivo_path}"
            )

            # TODO:
            # Aqui futuramente vamos buscar o arquivo no bucket
            # usando o caminho/link recebido em 'arquivo'.

    return jsonify({
        "success": False,
        "request_id": request_id,
        "message": f"A action '{validated_request.action}' não foi implementada para esse tipo de request.",
        "request_type": validated_request.request_type,
        "action": validated_request.action
    }), 400