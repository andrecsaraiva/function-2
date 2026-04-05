def obter_arquivo_request(arquivo):
    if arquivo is None:
        return None

    # pega tamanho sem perder o ponteiro do arquivo
    posicao_atual = arquivo.stream.tell()
    arquivo.stream.seek(0, 2)
    tamanho = arquivo.stream.tell()
    arquivo.stream.seek(0)

    return {
        "filename": arquivo.filename,
        "content_type": arquivo.content_type,
        "size_bytes": tamanho
    }