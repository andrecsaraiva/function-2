from flask import jsonify

def run(request):
    if request.method != "POST":
        return jsonify({"error": "Use POST"}), 405

    payload = request.get_json(silent=True) or {}
    name = payload.get("name", "World")

    return jsonify({"message": f"Developer {name}"}), 200