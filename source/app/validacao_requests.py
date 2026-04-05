from dataclasses import dataclass
from typing import Optional

@dataclass
class ValidatedRequest:
    request_type: str          # "json" ou "form-data"
    action: str
    data: dict
    arquivo: Optional[object] = None
    arquivo_path: Optional[str] = None

class ValidarRequests:
    def __init__(self, request, request_id: str):
        self.request = request
        self.request_id = request_id
        self.content_type = (request.content_type or "").lower()

    def validar(self) -> ValidatedRequest:
        body_type = self._detectar_tipo_corpo()

        if body_type == "json":
            return self._validar_json()

        if body_type == "form-data":
            return self._validar_form_data()

        raise ValueError("Tipo de corpo não suportado. Use application/json ou multipart/form-data.")

    def _detectar_tipo_corpo(self) -> str:
        if "multipart/form-data" in self.content_type:
            return "form-data"

        if self.request.is_json or "application/json" in self.content_type:
            return "json"

        # fallback simples
        if self.request.files or self.request.form:
            return "form-data"

        if self.request.get_json(silent=True) is not None:
            return "json"

        raise ValueError("Não foi possível identificar o tipo do corpo da requisição.")

    def _validar_json(self) -> ValidatedRequest:
        data = self.request.get_json(silent=True) or {}

        action = str(data.get("action", "")).strip()
        if not action:
            raise ValueError("No JSON, o parâmetro 'action' é obrigatório.")

        arquivo_path = data.get("arquivo")
        if arquivo_path is not None:
            arquivo_path = str(arquivo_path).strip() or None

        return ValidatedRequest(
            request_type="json",
            action=action,
            data=data,
            arquivo=None,
            arquivo_path=arquivo_path
        )

    def _validar_form_data(self) -> ValidatedRequest:
        data = self.request.form.to_dict(flat=True)

        action = str(data.get("action", "")).strip()
        if not action:
            raise ValueError("No form-data, o parâmetro 'action' é obrigatório.")

        arquivo = self.request.files.get("arquivo")
        if arquivo is None or not arquivo.filename:
            raise ValueError("No form-data, o arquivo é obrigatório no campo 'arquivo'.")

        return ValidatedRequest(
            request_type="form-data",
            action=action,
            data=data,
            arquivo=arquivo,
            arquivo_path=None
        )