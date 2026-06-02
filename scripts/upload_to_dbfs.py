"""
WorldCup Data Platform
Script: upload_to_dbfs.py
==========================
Lê as credenciais do arquivo .env e envia recursivamente
todos os arquivos de data/raw/ e data/seeds/ para o DBFS do Databricks.

Suporta arquivos de qualquer tamanho (usa a API chunked do DBFS).
"""

import base64
import os
import sys
from pathlib import Path
import requests
from dotenv import load_dotenv

# Configuração de logs simples e bonita
class Logger:
    @staticmethod
    def info(msg):
        print(f"[INFO] {msg}")
    @staticmethod
    def success(msg):
        print(f"[SUCCESS] {msg}")
    @staticmethod
    def warning(msg):
        print(f"[WARNING] {msg}")
    @staticmethod
    def error(msg):
        print(f"[ERROR] {msg}", file=sys.stderr)

# Carrega variáveis de ambiente
ROOT_DIR = Path(__file__).parent.parent
load_dotenv(ROOT_DIR / ".env")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").strip().rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "").strip()

if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
    Logger.error("Erro: Credenciais do Databricks não encontradas no arquivo .env!")
    Logger.info("Por favor, configure DATABRICKS_HOST e DATABRICKS_TOKEN no arquivo .env.")
    sys.exit(1)

# Normaliza host caso falte o schema
if not DATABRICKS_HOST.startswith("http://") and not DATABRICKS_HOST.startswith("https://"):
    DATABRICKS_HOST = "https://" + DATABRICKS_HOST

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

def upload_file_to_dbfs(local_file_path: Path, dbfs_path: str):
    """Envia um arquivo local para o DBFS usando chunks de no máximo 1MB."""
    try:
        # 1. Cria a sessão do arquivo no DBFS
        create_url = f"{DATABRICKS_HOST}/api/2.0/dbfs/create"
        payload = {
            "path": dbfs_path,
            "overwrite": True
        }
        res = requests.post(create_url, json=payload, headers=HEADERS)
        if res.status_code != 200:
            Logger.error(f"Erro ao criar arquivo no DBFS: {res.text}")
            return False
        
        handle = res.json()["handle"]
        
        # 2. Upload em blocos (chunks de 1MB)
        chunk_size = 1024 * 1024  # 1 MB
        add_block_url = f"{DATABRICKS_HOST}/api/2.0/dbfs/add-block"
        
        with open(local_file_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                
                # Codifica o chunk em base64 string
                b64_data = base64.b64encode(chunk).decode("utf-8")
                
                block_payload = {
                    "handle": handle,
                    "data": b64_data
                }
                res = requests.post(add_block_url, json=block_payload, headers=HEADERS)
                if res.status_code != 200:
                    Logger.error(f"Erro ao enviar bloco do arquivo {local_file_path.name}: {res.text}")
                    # Tenta fechar a sessão em caso de erro
                    requests.post(f"{DATABRICKS_HOST}/api/2.0/dbfs/close", json={"handle": handle}, headers=HEADERS)
                    return False
        
        # 3. Fecha a sessão
        close_url = f"{DATABRICKS_HOST}/api/2.0/dbfs/close"
        res = requests.post(close_url, json={"handle": handle}, headers=HEADERS)
        if res.status_code != 200:
            Logger.error(f"Erro ao fechar sessão no DBFS: {res.text}")
            return False
            
        return True
    except Exception as e:
        Logger.error(f"Exceção durante upload de {local_file_path.name}: {e}")
        return False

def main():
    print("=" * 60)
    print("      WorldCup Data Platform — Databricks DBFS Uploader")
    print("=" * 60)
    Logger.info(f"Host: {DATABRICKS_HOST}")
    
    # Mapeamento de diretórios locais -> caminhos DBFS
    # Usando /FileStore/wc-platform/ para alinhar com os notebooks Bronze/Silver/Gold
    upload_targets = [
        (ROOT_DIR / "data" / "raw", "/FileStore/wc-platform/raw"),
        (ROOT_DIR / "data" / "seeds", "/FileStore/wc-platform/seeds")
    ]
    
    total_files = 0
    uploaded_files = 0
    failed_files = 0
    
    # Primeiro conta os arquivos
    files_to_upload = []
    for local_dir, dbfs_base in upload_targets:
        if not local_dir.exists():
            Logger.warning(f"Diretório local {local_dir} não existe, pulando.")
            continue
            
        for path in local_dir.rglob("*"):
            if path.is_file():
                # Calcula caminho relativo
                relative_path = path.relative_to(local_dir)
                # Formata caminho DBFS de destino com barras padrão
                dbfs_dest_path = f"{dbfs_base}/{relative_path.as_posix()}"
                files_to_upload.append((path, dbfs_dest_path))
    
    total_files = len(files_to_upload)
    Logger.info(f"Total de arquivos encontrados para upload: {total_files}")
    
    if total_files == 0:
        Logger.warning("Nenhum arquivo encontrado para upload. Execute os scrapers primeiro!")
        return

    for idx, (local_path, dbfs_path) in enumerate(files_to_upload, 1):
        rel_str = local_path.relative_to(ROOT_DIR)
        print(f"[{idx}/{total_files}] Enviando {rel_str} ...", end="", flush=True)
        
        success = upload_file_to_dbfs(local_path, dbfs_path)
        if success:
            print("\r" + " " * 80 + "\r" + f"  [OK] [{idx}/{total_files}] Sucesso: {rel_str} -> {dbfs_path}")
            uploaded_files += 1
        else:
            print("\r" + " " * 80 + "\r" + f"  [FAIL] [{idx}/{total_files}] Falha: {rel_str}")
            failed_files += 1

    print("=" * 60)
    if failed_files == 0:
        Logger.success(f"Upload concluído com sucesso! {uploaded_files}/{total_files} arquivos enviados.")
    else:
        Logger.warning(f"Upload finalizado com avisos. Sucessos: {uploaded_files}, Falhas: {failed_files}.")
    print("=" * 60)

if __name__ == "__main__":
    main()
