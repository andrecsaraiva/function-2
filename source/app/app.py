from flask import jsonify
from app.gcp_connection import GcpConnection


def run(request):
    if request.method != "POST":
        return jsonify({"error": "Use POST"}), 405

    try:
        conn = GcpConnection()
        row = conn.insert_test_row()

        return jsonify({
            "message": "Linha inserida com sucesso no BigQuery.",
            "table": conn.table_id,
            "row": row
        }), 200

    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500