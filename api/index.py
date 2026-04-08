from flask import Flask, request, jsonify
import pandas as pd
import numpy as np

app = Flask(__name__)

def limpar_moeda(valor):
    if pd.isna(valor): return 0.0
    if isinstance(valor, (int, float)): return float(valor)
    v = str(valor).replace('R$', '').strip()
    if '.' in v and ',' not in v:
        v = v.replace('.', '')
    elif '.' in v and ',' in v:
        if v.rfind(',') > v.rfind('.'):
            v = v.replace('.', '').replace(',', '.')
        else:
            v = v.replace(',', '')
    elif ',' in v:
        v = v.replace(',', '.')
    try: return float(v)
    except: return 0.0

def carregar_planilha_segura(arquivo, is_ads=False):
    nome = arquivo.filename.lower()
    
    if nome.endswith('.csv'):
        df = pd.read_csv(arquivo, header=None)
    else:
        try:
            df = pd.read_excel(arquivo, sheet_name='Relatório Anúncios patrocinados' if is_ads else 'Relatório', header=None)
        except Exception:
            arquivo.seek(0)
            df = pd.read_excel(arquivo, header=None)
            
    linha_cabecalho = 0
    
    for i in range(min(30, len(df))):
        linha_atual = df.iloc[i].astype(str).str.lower().tolist()
        if is_ads:
            if any('código do anúncio' in v or 'número do anúncio vendido' in v for v in linha_atual):
                linha_cabecalho = i
                break
        else:
            if any('id do anúncio' in v for v in linha_atual):
                linha_cabecalho = i
                break
            
    df.columns = df.iloc[linha_cabecalho]
    df = df.iloc[linha_cabecalho + 1:].reset_index(drop=True)
    df.columns = [str(col).strip().replace('\n', ' ') for col in df.columns]
    return df

# ROTA ALTERADA PARA O PADRÃO VERCEL (/api/processar)
@app.route('/api/processar', methods=['POST'])
def processar():
    try:
        arq_desempenho = request.files.get('desempenho')
        arq_ads = request.files.get('ads')

        if not arq_desempenho or not arq_ads:
            return jsonify({"erro": "Selecione os dois relatórios para iniciar a análise."}), 400

        df_desempenho = carregar_planilha_segura(arq_desempenho, False)
        df_ads = carregar_planilha_segura(arq_ads, True)

        col_vendas_brutas = 'Vendas brutas (BRL)'
        col_unidades = 'Unidades vendidas'
        
        col_id_ads = next((c for c in df_ads.columns if 'código do anúncio' in c.lower() or 'número do anúncio vendido' in c.lower()), None)
        col_receita_ads = next((c for c in df_ads.columns if 'receita' in c.lower() and 'moeda local' in c.lower() and 'diretas' not in c.lower()), 'Receita')
        col_invest_ads = next((c for c in df_ads.columns if 'investimento' in c.lower() and 'moeda local' in c.lower()), None)

        if not col_id_ads:
            return jsonify({"erro": "Não foi possível encontrar a coluna de ID na planilha de Ads."}), 400

        if col_vendas_brutas in df_desempenho.columns:
            df_desempenho[col_vendas_brutas] = df_desempenho[col_vendas_brutas].apply(limpar_moeda)
        
        if col_unidades in df_desempenho.columns:
            df_desempenho[col_unidades] = pd.to_numeric(df_desempenho[col_unidades], errors='coerce').fillna(0)
        else:
            df_desempenho[col_unidades] = 0

        if col_receita_ads in df_ads.columns:
            df_ads[col_receita_ads] = df_ads[col_receita_ads].apply(limpar_moeda)
        if col_invest_ads and col_invest_ads in df_ads.columns:
            df_ads[col_invest_ads] = df_ads[col_invest_ads].apply(limpar_moeda)

        df_ads['ID_Tratado'] = df_ads[col_id_ads].astype(str).str.replace('MLB', '', regex=False)
        df_desempenho['ID_Tratado'] = df_desempenho['ID do anúncio'].astype(str)

        agg_dict = {col_receita_ads: 'sum'}
        if col_invest_ads:
            agg_dict[col_invest_ads] = 'sum'

        df_ads_agrupado = df_ads.groupby('ID_Tratado').agg(agg_dict).reset_index()
        df_ads_agrupado.rename(columns={col_receita_ads: 'Receita_Ads'}, inplace=True)
        
        if col_invest_ads:
            df_ads_agrupado.rename(columns={col_invest_ads: 'Investimento_Ads'}, inplace=True)
        else:
            df_ads_agrupado['Investimento_Ads'] = 0.0
        
        df_desempenho['Anúncio'] = df_desempenho['Anúncio'].fillna('Anúncio sem título')
        
        df_desempenho_agrupado = df_desempenho.groupby('ID_Tratado').agg({
            'Anúncio': 'first', 
            col_vendas_brutas: 'sum',
            col_unidades: 'sum'
        }).reset_index()

        df_desempenho_agrupado = df_desempenho_agrupado.sort_values(by=col_vendas_brutas, ascending=False).copy()
        faturamento_total = float(df_desempenho_agrupado[col_vendas_brutas].sum())
        unidades_total = int(df_desempenho_agrupado[col_unidades].sum())
        
        if faturamento_total > 0:
            df_desempenho_agrupado['Percentual_Acumulado'] = (df_desempenho_agrupado[col_vendas_brutas].cumsum() / faturamento_total) * 100
        else:
            df_desempenho_agrupado['Percentual_Acumulado'] = 0
            
        condicoes = [(df_desempenho_agrupado['Percentual_Acumulado'] <= 80), (df_desempenho_agrupado['Percentual_Acumulado'] > 80) & (df_desempenho_agrupado['Percentual_Acumulado'] <= 95)]
        df_desempenho_agrupado['Curva_ABC'] = np.select(condicoes, ['A', 'B'], default='C')

        df_final = pd.merge(df_desempenho_agrupado, df_ads_agrupado, on='ID_Tratado', how='left')
        df_final['Receita_Ads'] = df_final['Receita_Ads'].fillna(0)
        df_final['Investimento_Ads'] = df_final.get('Investimento_Ads', 0).fillna(0)
        
        df_final['Dependencia_Ads'] = np.where(df_final[col_vendas_brutas] > 0, (df_final['Receita_Ads'] / df_final[col_vendas_brutas]) * 100, 0)
        df_final['Dependencia_Ads'] = np.minimum(df_final['Dependencia_Ads'], 100) 

        df_final['Alerta_Oportunidade'] = (df_final['Curva_ABC'] == 'A') & (df_final['Receita_Ads'] == 0)
        df_final['Alerta_Gargalo'] = (df_final['Curva_ABC'] == 'C') & (df_final['Receita_Ads'] > 0)

        df_final = df_final.replace([np.inf, -np.inf], 0).fillna(0)

        oportunidades = df_final[df_final['Alerta_Oportunidade']][['ID_Tratado', 'Anúncio', col_unidades, col_vendas_brutas]].rename(columns={col_vendas_brutas: 'Faturamento', col_unidades: 'Unidades'}).to_dict('records')
        gargalos = df_final[df_final['Alerta_Gargalo']][['ID_Tratado', 'Anúncio', col_unidades, col_vendas_brutas, 'Receita_Ads', 'Investimento_Ads', 'Dependencia_Ads']].rename(columns={col_vendas_brutas: 'Faturamento', col_unidades: 'Unidades'}).sort_values(by='Investimento_Ads', ascending=False).to_dict('records')
        visao_geral = df_final.sort_values(by=col_vendas_brutas, ascending=False)[['ID_Tratado', 'Anúncio', 'Curva_ABC', col_unidades, col_vendas_brutas, 'Receita_Ads', 'Investimento_Ads', 'Dependencia_Ads']].rename(columns={col_vendas_brutas: 'Faturamento', col_unidades: 'Unidades'}).to_dict('records')

        return jsonify({
            "kpis": {
                "faturamento_total": faturamento_total,
                "unidades_total": unidades_total,
                "receita_ads": float(df_final['Receita_Ads'].sum()),
                "investimento_ads": float(df_final['Investimento_Ads'].sum()),
                "qtd_oportunidades": len(oportunidades)
            },
            "oportunidades": oportunidades,
            "gargalos": gargalos,
            "visao_geral": visao_geral
        })
    except Exception as e:
        return jsonify({"erro": f"Erro na leitura. Detalhe: {str(e)}"}), 500

# O Vercel cuida do app.run() automaticamente, não precisa colocar aqui.