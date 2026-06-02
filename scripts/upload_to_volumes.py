"""
WorldCup Data Platform
Script: upload_to_volumes.py
=============================
Lê as credenciais do arquivo .env e envia recursivamente
todos os arquivos de data/raw/ e data/seeds/ para um Volume do Unity Catalog no Databricks.

Usa a moderna Files API (/api/2.0/fs/files/Volumes/...) recomendada pela Databricks.
"""

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

def upload_file_to_volume(local_file_path: Path, volume_dest_path: str):
    """Envia um arquivo local para o Unity Catalog Volume usando a Files API (HTTP PUT)."""
    try:
        # volume_dest_path ex: "Volumes/lakehouse/wc_platform/files/raw/fbref/misc_2024-25.parquet"
        url = f"{DATABRICKS_HOST}/api/2.0/fs/files/{volume_dest_path.lstrip('/')}"
        
        with open(local_file_path, "rb") as f:
            headers = {
                "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                "Content-Type": "application/octet-stream"
            }
            # O parâmetro overwrite=true garante a atualização idempotente
            res = requests.put(f"{url}?overwrite=true", data=f, headers=headers)
            if res.status_code not in (200, 201, 204):
                Logger.error(f"Erro ao enviar {local_file_path.name} para Volume (Status {res.status_code}): {res.text}")
                return False
        return True
    except Exception as e:
        Logger.error(f"Exceção durante upload de {local_file_path.name}: {e}")
        return False

def main():
    print("=" * 60)
    print("   WorldCup Data Platform — Databricks UC Volumes Uploader")
    print("=" * 60)
    Logger.info(f"Host: {DATABRICKS_HOST}")
    
    # Mapeamento de diretórios locais -> caminhos nos Volumes do Unity Catalog
    # Usando o Volume padrão: /Volumes/main/default/wc_platform
    upload_targets = [
        (ROOT_DIR / "data" / "raw", "Volumes/lakehouse/wc_platform/files/raw"),
        (ROOT_DIR / "data" / "seeds", "Volumes/lakehouse/wc_platform/files/seeds")
    ]
    
    total_files = 0
    uploaded_files = 0
    failed_files = 0
    
    # Primeiro conta e mapeia os arquivos
    files_to_upload = []
    for local_dir, volume_base in upload_targets:
        if not local_dir.exists():
            Logger.warning(f"Diretório local {local_dir} não existe, pulando.")
            continue
            
        for path in local_dir.rglob("*"):
            if path.is_file():
                # Calcula caminho relativo
                relative_path = path.relative_to(local_dir)
                # Formata caminho do Volume de destino
                volume_dest_path = f"{volume_base}/{relative_path.as_posix()}"
                files_to_upload.append((path, volume_dest_path))
    
    total_files = len(files_to_upload)
    Logger.info(f"Total de arquivos encontrados para upload: {total_files}")
    
    if total_files == 0:
        Logger.warning("Nenhum arquivo encontrado para upload. Execute os scrapers primeiro!")
        return

    Logger.info("Iniciando upload para os Volumes do Unity Catalog...")
    for idx, (local_path, volume_path) in enumerate(files_to_upload, 1):
        rel_str = local_path.relative_to(ROOT_DIR)
        print(f"[{idx}/{total_files}] Enviando {rel_str} ...", end="", flush=True)
        
        success = upload_file_to_volume(local_path, volume_path)
        if success:
            print("\r" + " " * 80 + "\r" + f"  [OK] [{idx}/{total_files}] Sucesso: {rel_str} -> /{volume_path}")
            uploaded_files += 1
        else:
            print("\r" + " " * 80 + "\r" + f"  [FAIL] [{idx}/{total_files}] Falha: {rel_str}")
            failed_files += 1

    print("=" * 60)
    if failed_files == 0:
        Logger.success(f"Upload concluído com sucesso! {uploaded_files}/{total_files} arquivos enviados.")
    else:
        Logger.warning(f"Upload finalizado com avisos. Sucessos: {uploaded_files}, Falhas: {failed_files}.")
        Logger.info("Dica: Certifique-se de ter criado o volume rodando 'CREATE VOLUME lakehouse.wc_platform.files;' no Databricks.")
    print("=" * 60)

if __name__ == "__main__":
    main()
