"""
WorldCup Data Platform
Script: migrate_all_to_lakehouse.py
====================================
Adapta todo o projeto local (Notebooks e Script de Upload) para usar o catálogo real
'lakehouse' e o volume '/Volumes/lakehouse/wc_platform/files' identificados no workspace.
"""

from pathlib import Path

def log_info(msg): print(f"[INFO] {msg}")
def log_success(msg): print(f"[SUCCESS] {msg}")

ROOT_DIR = Path(__file__).parent.parent
NOTEBOOKS_DIR = ROOT_DIR / "notebooks"
UPLOAD_SCRIPT = ROOT_DIR / "scripts" / "upload_to_volumes.py"

def migrate_notebook(filepath: Path):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
        
    original = content
    
    # 1. Substitui caminhos de storage (DBFS legado e Volume antigo 'main')
    content = content.replace("dbfs:/FileStore/wc-platform", "/Volumes/lakehouse/wc_platform/files")
    content = content.replace("/Volumes/main/default/wc_platform", "/Volumes/lakehouse/wc_platform/files")
    
    # 2. Substitui o nome do Catálogo nas declarações de constantes
    content = content.replace('CATALOG    = "worldcup"', 'CATALOG    = "lakehouse"')
    content = content.replace('CATALOG   = "worldcup"', 'CATALOG   = "lakehouse"')
    content = content.replace('CATALOG = "worldcup"', 'CATALOG = "lakehouse"')
    
    if content != original:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
        return True
    return False

def migrate_upload_script():
    if not UPLOAD_SCRIPT.exists():
        return False
        
    with open(UPLOAD_SCRIPT, "r", encoding="utf-8") as f:
        content = f.read()
        
    original = content
    
    # Substitui os alvos de upload no Volume
    content = content.replace("Volumes/main/default/wc_platform/raw", "Volumes/lakehouse/wc_platform/files/raw")
    content = content.replace("Volumes/main/default/wc_platform/seeds", "Volumes/lakehouse/wc_platform/files/seeds")
    
    if content != original:
        with open(UPLOAD_SCRIPT, "w", encoding="utf-8") as f:
            f.write(content)
        return True
    return False

def main():
    print("=" * 60)
    print("     WorldCup Data Platform — Lakehouse Catalog Adapter")
    print("=" * 60)
    
    # 1. Migra os notebooks recursivamente
    log_info("Migrando notebooks (Bronze, Silver, Gold)...")
    notebook_count = 0
    for file in NOTEBOOKS_DIR.rglob("*.py"):
        if migrate_notebook(file):
            log_success(f"  Notebook adaptado: {file.relative_to(ROOT_DIR)}")
            notebook_count += 1
            
    # 2. Migra o script de upload
    log_info("Migrando script de upload...")
    if migrate_upload_script():
        log_success("  Script de upload adaptado!")
        
    print("=" * 60)
    log_success(f"Migração completa! {notebook_count} notebooks e o script de upload foram adaptados para o catálogo 'lakehouse'.")
    print("=" * 60)

if __name__ == "__main__":
    main()
