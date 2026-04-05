import logging
from uuid import uuid4
from flask import jsonify

from app.app import process_action
from app.validacao_requests import ValidarRequests

logging.basicConfig(level=logging.INFO)


def main(request):
    return process(request)


def process(request):
    request_id = str(uuid4())
    logging.info(f"[{request_id}] Início da requisição")

    try:
        validated_request = ValidarRequests(
            request=request,
            request_id=request_id
        ).validar()

        return process_action(
            validated_request=validated_request,
            request_id=request_id
        )

    except ValueError as e:
        logging.warning(f"[{request_id}] Erro de validação: {e}")
        return jsonify({
            "success": False,
            "request_id": request_id,
            "error": str(e)
        }), 400

    except Exception as e:
        logging.exception(f"[{request_id}] Erro interno: {e}")
        return jsonify({
            "success": False,
            "request_id": request_id,
            "error": str(e)
        }), 500