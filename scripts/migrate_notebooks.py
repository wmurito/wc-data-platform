"""
WorldCup Data Platform
Script: migrate_notebooks.py
=============================
Migra os caminhos dos notebooks locais da pasta notebooks/bronze/
para lerem do Volume do Unity Catalog (/Volumes/main/default/wc_platform)
em vez do DBFS legado desabilitado.
"""

from pathlib import Path

# Configuração de logs
def log_info(msg): print(f"[INFO] {msg}")
def log_success(msg): print(f"[SUCCESS] {msg}")

ROOT_DIR = Path(__file__).parent.parent
NOTEBOOKS_DIR = ROOT_DIR / "notebooks" / "bronze"

def migrate_file(filepath: Path):
    if not filepath.exists():
        return False
        
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
        
    # Padrão antigo: dbfs:/FileStore/wc-platform
    # Padrão novo: /Volumes/main/default/wc_platform
    old_target = "dbfs:/FileStore/wc-platform"
    new_target = "/Volumes/main/default/wc_platform"
    
    if old_target in content:
        new_content = content.replace(old_target, new_target)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(new_content)
        return True
    return False

def main():
    print("=" * 60)
    print("      WorldCup Data Platform — Notebook Path Migrator")
    print("=" * 60)
    
    if not NOTEBOOKS_DIR.exists():
        log_info(f"Diretório {NOTEBOOKS_DIR} não encontrado.")
        return
        
    migrated_count = 0
    for file in NOTEBOOKS_DIR.glob("*.py"):
        success = migrate_file(file)
        if success:
            log_success(f"Migrado com sucesso: {file.name}")
            migrated_count += 1
        else:
            log_info(f"Sem alterações necessárias: {file.name}")
            
    print("=" * 60)
    log_success(f"Migração completa! {migrated_count} notebooks atualizados localmente.")
    print("=" * 60)

if __name__ == "__main__":
    main()
