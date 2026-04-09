from flask import Flask, request, jsonify
import pandas as pd
import numpy as np

app = Flask(__name__)

def limpar_moeda(valor):
    if pd.isna(valor) or valor == '-': return 0.0
    if isinstance(valor, (int, float)): return float(valor)
    
    v = str(valor).replace('R$', '').replace('BRL', '').strip()
    v = v.replace(' ', '')
    
    if not v or v == '-': return 0.0
    
    if ',' in v and '.' in v:
        if v.rfind(',') > v.rfind('.'):
            v = v.replace('.', '').replace(',', '.')
        else:
            v = v.replace(',', '')
    elif ',' in v:
        v = v.replace(',', '.')
    elif v.count('.') > 1:
        v = v.replace('.', '')

    try: 
        return float(v)
    except: 
        return 0.0

def carregar_planilha_segura(arquivo, is_ads=False):
    nome = arquivo.filename.lower()
    
    if nome.endswith('.csv'):
        # BLINDAGEM DE ENCODING: Tenta ler nos formatos mais comuns do ML
        try:
            df = pd.read_csv(arquivo, header=None, encoding='utf-8')
        except UnicodeDecodeError:
            arquivo.seek(0)
            try:
                df = pd.read_csv(arquivo, header=None, encoding='utf-16')
            except UnicodeDecodeError:
                arquivo.seek(0)
                df = pd.read_csv(arquivo, header=None, encoding='iso-8859-1')
    else:
        try:
            df = pd.read_excel(arquivo, sheet_name='Relatório Anúncios patrocinados' if is_ads else 'Relatório', header=None)
        except Exception:
            arquivo.seek(0)
            df = pd.read_excel(arquivo, header=None)
            
    # Trava de segurança para arquivos vazios
    if df is None or len(df) == 0:
        raise ValueError("O arquivo enviado parece estar vazio.")
            
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

@app.route('/api/processar', methods=['POST'])
def processar():
    try:
        arq_desempenho = request.files.get('desempenho')
        arq_ads = request.files.get('ads')

        if not arq_desempenho:
            return jsonify({"erro": "A planilha de Desempenho é obrigatória para qualquer análise."}), 400

        # ======= 1. PROCESSAMENTO DO DESEMPENHO =======
        df_desempenho = carregar_planilha_segura(arq_desempenho, False)
        
        col_id_des = next((c for c in df_desempenho.columns if 'id do anúncio' in c.lower()), 'ID do anúncio')
        col_vendas_brutas = next((c for c in df_desempenho.columns if 'vendas brutas' in c.lower() or 'receita' in c.lower()), 'Vendas brutas (BRL)')
        col_unidades = next((c for c in df_desempenho.columns if 'unidades' in c.lower() and 'vendidas' in c.lower()), 'Unidades vendidas')
        col_titulo_des = next((c for c in df_desempenho.columns if 'título' in c.lower() or 'anúncio' in c.lower() and 'id' not in c.lower()), 'Anúncio')
        
        df_desempenho = df_desempenho[df_desempenho[col_id_des].astype(str).str.contains(r'\d', regex=True, na=False)]

        if col_vendas_brutas in df_desempenho.columns:
            df_desempenho[col_vendas_brutas] = df_desempenho[col_vendas_brutas].apply(limpar_moeda)
        
        if col_unidades in df_desempenho.columns:
            df_desempenho[col_unidades] = pd.to_numeric(df_desempenho[col_unidades], errors='coerce').fillna(0)
        else:
            df_desempenho[col_unidades] = 0

        df_desempenho['ID_Tratado'] = df_desempenho[col_id_des].astype(str).str.upper().str.replace('MLB', '', regex=False).str.replace(r'\.0$', '', regex=True).str.strip()
        df_desempenho['Anúncio_Clean'] = df_desempenho.get(col_titulo_des, df_desempenho.get('Anúncio', 'Anúncio sem título')).fillna('Anúncio sem título')
        
        df_desempenho_agrupado = df_desempenho.groupby('ID_Tratado').agg({
            'Anúncio_Clean': 'first', 
            col_vendas_brutas: 'sum',
            col_unidades: 'sum'
        }).reset_index()
        
        df_desempenho_agrupado.rename(columns={'Anúncio_Clean': 'Anúncio'}, inplace=True)
        df_desempenho_agrupado = df_desempenho_agrupado.sort_values(by=col_vendas_brutas, ascending=False).copy()
        
        faturamento_total = float(df_desempenho_agrupado[col_vendas_brutas].sum())
        unidades_total = int(df_desempenho_agrupado[col_unidades].sum())
        
        if faturamento_total > 0:
            df_desempenho_agrupado['Percentual_Acumulado'] = (df_desempenho_agrupado[col_vendas_brutas].cumsum() / faturamento_total) * 100
        else:
            df_desempenho_agrupado['Percentual_Acumulado'] = 0
            
        condicoes = [(df_desempenho_agrupado['Percentual_Acumulado'] <= 80), (df_desempenho_agrupado['Percentual_Acumulado'] > 80) & (df_desempenho_agrupado['Percentual_Acumulado'] <= 95)]
        df_desempenho_agrupado['Curva_ABC'] = np.select(condicoes, ['A', 'B'], default='C')

        # ======= 2. VARIÁVEIS GLOBAIS =======
        has_ads = False
        oportunidades = []
        gargalos = []
        receita_ads_total = 0.0
        investimento_ads_total = 0.0

        # ======= 3. PROCESSAMENTO DO ADS (SE EXISTIR) =======
        if arq_ads:
            has_ads = True
            df_ads = carregar_planilha_segura(arq_ads, True)
            
            col_id_ads = next((c for c in df_ads.columns if 'código do anúncio' in c.lower() or 'número do anúncio vendido' in c.lower()), None)
            col_receita_ads = next((c for c in df_ads.columns if 'receita' in c.lower() and 'moeda local' in c.lower() and 'diretas' not in c.lower()), 'Receita')
            col_invest_ads = next((c for c in df_ads.columns if 'investimento' in c.lower() and 'moeda local' in c.lower()), None)
            col_titulo_ads = next((c for c in df_ads.columns if 'título' in c.lower() and 'anúncio' in c.lower()), 'Título_Ads')

            if not col_id_ads:
                return jsonify({"erro": "Não foi possível encontrar a coluna de ID na planilha de Ads."}), 400

            df_ads = df_ads[df_ads[col_id_ads].astype(str).str.contains(r'\d', regex=True, na=False)]

            if col_receita_ads in df_ads.columns:
                df_ads[col_receita_ads] = df_ads[col_receita_ads].apply(limpar_moeda)
            if col_invest_ads and col_invest_ads in df_ads.columns:
                df_ads[col_invest_ads] = df_ads[col_invest_ads].apply(limpar_moeda)

            df_ads['ID_Tratado'] = df_ads[col_id_ads].astype(str).str.upper().str.replace('MLB', '', regex=False).str.replace(r'\.0$', '', regex=True).str.strip()
            df_ads['Anuncio_Ads_Temp'] = df_ads.get(col_titulo_ads, 'Sem Título')

            agg_dict = {col_receita_ads: 'sum', 'Anuncio_Ads_Temp': 'first'}
            if col_invest_ads:
                agg_dict[col_invest_ads] = 'sum'

            df_ads_agrupado = df_ads.groupby('ID_Tratado').agg(agg_dict).reset_index()
            df_ads_agrupado.rename(columns={col_receita_ads: 'Receita_Ads'}, inplace=True)
            
            if col_invest_ads:
                df_ads_agrupado.rename(columns={col_invest_ads: 'Investimento_Ads'}, inplace=True)
            else:
                df_ads_agrupado['Investimento_Ads'] = 0.0

            # BLINDAGEM DO JOIN: OUTER JOIN para não perder campanhas de produtos com 0 vendas orgânicas
            df_final = pd.merge(df_desempenho_agrupado, df_ads_agrupado, on='ID_Tratado', how='outer')
            
            # Preenchendo os "órfãos" (produtos que gastaram Ads mas não estavam no Desempenho)
            df_final['Anúncio'] = df_final['Anúncio'].fillna(df_final['Anuncio_Ads_Temp']).fillna('Produto apenas em Ads')
            df_final['Curva_ABC'] = df_final['Curva_ABC'].fillna('C')
            df_final[col_vendas_brutas] = df_final[col_vendas_brutas].fillna(0.0)
            df_final[col_unidades] = df_final[col_unidades].fillna(0)
            df_final['Receita_Ads'] = df_final['Receita_Ads'].fillna(0.0)
            df_final['Investimento_Ads'] = df_final.get('Investimento_Ads', 0.0).fillna(0.0)
            
            # BLINDAGEM DA DIVISÃO: Se fat_total > 0 ok, senão verifica se tem receita_ads > 0 para fixar em 100%
            df_final['Dependencia_Ads'] = np.where(
                df_final[col_vendas_brutas] > 0, 
                (df_final['Receita_Ads'] / df_final[col_vendas_brutas]) * 100, 
                np.where(df_final['Receita_Ads'] > 0, 100, 0)
            )
            df_final['Dependencia_Ads'] = np.minimum(df_final['Dependencia_Ads'], 100) 

            df_final['Alerta_Oportunidade'] = (df_final['Curva_ABC'] == 'A') & (df_final['Receita_Ads'] == 0)
            df_final['Alerta_Gargalo'] = (df_final['Curva_ABC'] == 'C') & (df_final['Investimento_Ads'] > 0) & (df_final[col_vendas_brutas] <= df_final['Investimento_Ads'])

            df_final = df_final.replace([np.inf, -np.inf], 0).fillna(0)

            oportunidades = df_final[df_final['Alerta_Oportunidade']][['ID_Tratado', 'Anúncio', col_unidades, col_vendas_brutas]].rename(columns={col_vendas_brutas: 'Faturamento', col_unidades: 'Unidades'}).to_dict('records')
            
            # O gargalo agora ordena pelo MAIOR investimento em ads de produtos na curva C
            gargalos = df_final[df_final['Alerta_Gargalo']][['ID_Tratado', 'Anúncio', col_unidades, col_vendas_brutas, 'Receita_Ads', 'Investimento_Ads', 'Dependencia_Ads']].rename(columns={col_vendas_brutas: 'Faturamento', col_unidades: 'Unidades'}).sort_values(by='Investimento_Ads', ascending=False).to_dict('records')
            
            receita_ads_total = float(df_final['Receita_Ads'].sum())
            investimento_ads_total = float(df_final['Investimento_Ads'].sum())
            
            visao_geral = df_final.sort_values(by=col_vendas_brutas, ascending=False)[['ID_Tratado', 'Anúncio', 'Curva_ABC', col_unidades, col_vendas_brutas, 'Receita_Ads', 'Investimento_Ads', 'Dependencia_Ads']].rename(columns={col_vendas_brutas: 'Faturamento', col_unidades: 'Unidades'}).to_dict('records')
        
        # ======= 4. MODO ORGÂNICO (SEM ADS) =======
        else:
            df_final = df_desempenho_agrupado.copy()
            df_final['Receita_Ads'] = 0.0
            df_final['Investimento_Ads'] = 0.0
            df_final['Dependencia_Ads'] = 0.0
            visao_geral = df_final.sort_values(by=col_vendas_brutas, ascending=False)[['ID_Tratado', 'Anúncio', 'Curva_ABC', col_unidades, col_vendas_brutas, 'Receita_Ads', 'Investimento_Ads', 'Dependencia_Ads']].rename(columns={col_vendas_brutas: 'Faturamento', col_unidades: 'Unidades'}).to_dict('records')

        return jsonify({
            "has_ads": has_ads,
            "kpis": {
                "faturamento_total": faturamento_total,
                "unidades_total": unidades_total,
                "receita_ads": receita_ads_total,
                "investimento_ads": investimento_ads_total,
                "qtd_oportunidades": len(oportunidades)
            },
            "oportunidades": oportunidades,
            "gargalos": gargalos,
            "visao_geral": visao_geral
        })
    except Exception as e:
        import traceback
        print(traceback.format_exc()) # Para você ver o erro no painel do Vercel
        return jsonify({"erro": f"Erro na formatação dos dados. Detalhe: {str(e)}"}), 500
