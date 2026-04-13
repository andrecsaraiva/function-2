import logging
from uuid import uuid4
from flask import jsonify, make_response

from app.app import run
from app.validacao_requests import ValidarRequests

logging.basicConfig(level=logging.INFO)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "3600",
}


def _with_cors(response):
    response = make_response(response)
    for key, value in CORS_HEADERS.items():
        response.headers[key] = value
    return response


def main(request):
    return process(request)


def process(request):
    if request.method == "OPTIONS":
        return _with_cors(("", 204))

    request_id = str(uuid4())
    logging.info(f"[{request_id}] Início da requisição")

    try:
        validated_request = ValidarRequests(
            request=request,
            request_id=request_id
        ).validar()

        return _with_cors(
            run(
                validated_request=validated_request,
                request_id=request_id
            )
        )

    except ValueError as e:
        logging.warning(f"[{request_id}] Erro de validação: {e}")
        return _with_cors((
            jsonify({
                "success": False,
                "request_id": request_id,
                "error": str(e)
            }),
            400
        ))

    except Exception as e:
        logging.exception(f"[{request_id}] Erro interno: {e}")
        return _with_cors((
            jsonify({
                "success": False,
                "request_id": request_id,
                "error": str(e)
            }),
            500
        ))