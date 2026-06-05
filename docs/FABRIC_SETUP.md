# Fabric Setup — Integração Databricks → OneDrive → Fabric → Power BI

**Status:** Guia completo  
**Tempo estimado:** 30-45 minutos

---

## Visão geral do fluxo

```
Databricks CE          OneDrive              Microsoft Fabric
──────────────         ─────────────         ─────────────────────────
Gold Delta tables  →   CSV files        →    Lakehouse (Delta tables)
(notebook 01)          (upload manual)       (notebook 02)
                                                    ↓
                                             Power BI Reports
                                             (Semantic Model)
```

---

## PASSO 1 — Exportar CSVs no Databricks

**Notebook:** `notebooks/serving/01_export_gold_to_csv.py`

1. Abrir Databricks Community Edition
2. Importar e rodar `01_export_gold_to_csv.py`
3. O notebook gera 9 arquivos CSV em `dbfs:/FileStore/wc-platform/export/`
4. Fazer download de cada arquivo via URL gerada pelo notebook:
   ```
   https://<seu-workspace>.azuredatabricks.net/files/wc-platform/export/simulation_results.csv
   ```
   Ou via Databricks CLI:
   ```bash
   databricks fs cp dbfs:/FileStore/wc-platform/export/ ./gold_exports/ --recursive
   ```

**Arquivos gerados:**

| Arquivo | Linhas aprox. | Tamanho |
|---|---|---|
| `simulation_results.csv` | 48 | ~5 KB |
| `top_scorers.csv` | ~500 | ~80 KB |
| `top_scorers_cross_tournament.csv` | ~300 | ~40 KB |
| `team_style.csv` | ~130 | ~25 KB |
| `team_style_overall.csv` | ~60 | ~10 KB |
| `match_features.csv` | ~262 | ~120 KB |
| `standings.csv` | ~130 | ~30 KB |
| `xg_predictions.csv` | ~15.000 | ~2 MB |
| `match_predictions.csv` | ~262 | ~40 KB |

---

## PASSO 2 — Upload para o OneDrive

1. Abrir **OneDrive** (onedrive.live.com ou via Microsoft 365)
2. Criar pasta: `WC-DataPlatform/gold-exports/`
3. Fazer upload dos 9 arquivos CSV para essa pasta
4. Confirmar que todos aparecem na pasta

---

## PASSO 3 — Criar o Lakehouse no Fabric

1. Abrir **Microsoft Fabric** → seu workspace
2. Clicar em **+ New Item** → **Lakehouse**
3. Nome: `WC_DataPlatform`
4. Clicar em **Create**

---

## PASSO 4 — Criar Shortcut do OneDrive no Lakehouse

O Shortcut conecta o Lakehouse diretamente ao OneDrive sem copiar os dados.

1. No Lakehouse `WC_DataPlatform`, clique em **Files** (painel esquerdo)
2. Clique em **...** → **New shortcut**
3. Selecione **OneDrive**
4. Autentique com sua conta Microsoft
5. Navegue até `WC-DataPlatform/gold-exports/`
6. Nome do shortcut: `wc-platform`
7. Clique **Create**

Agora os CSVs aparecem em `Files/wc-platform/` no Lakehouse.

---

## PASSO 5 — Rodar o notebook Fabric para criar as Delta Tables

1. No Lakehouse, clique em **Open notebook** → **New notebook**
2. Copiar o conteúdo de `notebooks/serving/02_fabric_lakehouse_setup.py`
3. Colar no notebook Fabric (ignorar as linhas `# MAGIC %md`)
4. Executar célula por célula
5. Verificar que as 9 tabelas `gold_*` aparecem em **Tables** no Lakehouse

---

## PASSO 6 — Criar o Semantic Model no Power BI

1. No Lakehouse, clicar em **New semantic model**
2. Nome: `WC_DataPlatform_Model`
3. Selecionar as tabelas:
   - `gold_simulation_results`
   - `gold_top_scorers`
   - `gold_standings`
   - `gold_team_style`
   - `gold_match_predictions`
   - `gold_xg_predictions`
   - Views: `vw_simulacao_copa2026`, `vw_artilharia_geral`, `vw_estilos_taticos`
4. Clicar **Confirm**

---

## PASSO 7 — Criar os Reports no Power BI

### Report 1 — Probabilidades Copa 2026

**Fonte:** `vw_simulacao_copa2026`

Visuais sugeridos:
- **Bar chart horizontal:** `team_name` × `pct_campeao` (Top 15)
- **Clustered bar:** `team_name` × `pct_semi`, `pct_final`, `pct_campeao`
- **Treemap:** `group` → `team_name`, tamanho = `pct_campeao`
- **Card:** país com maior % de título
- **Filtro:** por grupo (A-L)

---

### Report 2 — Artilharia e xG

**Fonte:** `vw_artilharia_geral`

Visuais sugeridos:
- **Scatter plot:** `xg_total` (eixo X) × `goals` (eixo Y), tamanho = `shots_total`, cor = `team_name`
  - Jogadores acima da diagonal = overperformers vs xG
  - Jogadores abaixo = underperformers
- **Table:** Top 20 artilheiros com colunas `goals`, `xg_total`, `goals_above_xg`, `assists`
- **Bar chart:** `competition` × média de `xg_total`
- **Filtro:** por competição, por posição

---

### Report 3 — Shot Map (xG por posição)

**Fonte:** `vw_shot_map_xg`

Visuais sugeridos:
- **Scatter plot:** `distance_to_goal` × `angle_to_goal`, cor = `is_goal`, tamanho = `our_xg`
- **Bar chart:** `shot_technique` × `our_xg` médio
- **Histogram:** distribuição de `our_xg` (0-1)
- **KPI cards:** total chutes, total gols, taxa de conversão, xG médio por chute

---

### Report 4 — Estilos Táticos

**Fonte:** `vw_estilos_taticos`

Visuais sugeridos:
- **Scatter plot:** `avg_ppda` (eixo X invertido) × `avg_possession_pct` (eixo Y)
  - Quadrantes: Alta posse + baixo PPDA = pressing alto | Alta posse + alto PPDA = posse sem pressão
  - Baixa posse + baixo PPDA = pressing compacto | Baixa posse + alto PPDA = contra-ataque
- **Bar chart:** `team_name` × `xg_diff_per_game` (saldo de xG por jogo)
- **Slicer:** por competição, por `tactical_profile`

---

## Atualização dos dados

Para atualizar os dados quando rodar novos notebooks Databricks:

1. Re-rodar `01_export_gold_to_csv.py` no Databricks
2. Fazer download dos CSVs atualizados
3. Substituir os arquivos no OneDrive (mesmo nome)
4. No Fabric: o Semantic Model atualiza automaticamente na próxima consulta
   (ou forçar: Semantic Model → Refresh → Refresh now)

---

## Checklist de validação no Power BI

```
[ ] simulation_results: 48 times | Argentina/France no topo
[ ] top_scorers WC 2022: Mbappé com 8 gols no rank 1
[ ] standings WC 2022: Argentina com max_stage = "Final"
[ ] xg_predictions: ~15.000 linhas | avg our_xg ~0.10
[ ] team_style: Brasil com tactical_profile preenchido
```
