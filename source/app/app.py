import logging
from flask import jsonify

from app.gcp_connection import GcpConnection
from app.obter_arquivo import obter_arquivo_request

def run(validated_request, request_id: str):
    logging.info(f"[{request_id}] Tipo do request: {validated_request.request_type}")
    logging.info(f"[{request_id}] Action recebida: {validated_request.action}")

    arquivo_info = None

    if validated_request.request_type == "form-data":
        arquivo_info = obter_arquivo_request(validated_request.arquivo)
        logging.info(f"[{request_id}] Arquivo recebido via form-data: {arquivo_info}")

        # TODO:
        # Aqui futuramente vamos ler o arquivo enviado no form-data
        # e processar o conteúdo conforme a action.

    elif validated_request.request_type == "json":
        if validated_request.arquivo_path:
            logging.info(
                f"[{request_id}] Caminho de arquivo recebido via JSON: {validated_request.arquivo_path}"
            )

            # TODO:
            # Aqui futuramente vamos buscar o arquivo no bucket
            # usando o caminho/link recebido em 'arquivo'.

    # Nesta etapa, ainda vamos apenas testar a conexão com BigQuery
    conn = GcpConnection()
    inserted_row = conn.insert_test_row()

    return jsonify({
        "success": True,
        "request_id": request_id,
        "message": "Requisição validada e linha inserida com sucesso no BigQuery.",
        "request_type": validated_request.request_type,
        "action": validated_request.action,
        "arquivo_info": arquivo_info,
        "arquivo_json": validated_request.arquivo_path,
        "table": conn.table_id,
        "inserted_row": inserted_row
    }), 200