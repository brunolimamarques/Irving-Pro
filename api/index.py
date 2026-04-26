from flask import Flask, request, jsonify
import pandas as pd
import numpy as np

app = Flask(__name__)

def limpar_moeda(valor):
    if pd.isna(valor) or valor == '-': return 0.0
    if isinstance(valor, (int, float)): return round(float(valor), 2)
    
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
        return round(float(v), 2)
    except: 
        return 0.0

def carregar_planilha_segura(arquivo, is_ads=False):
    nome = arquivo.filename.lower()
    
    if nome.endswith('.csv'):
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
            df = pd.read_excel(arquivo, header=None)
        except Exception:
            arquivo.seek(0)
            df = pd.read_excel(arquivo, header=None)
            
    if df is None or len(df) == 0:
        raise ValueError("O arquivo enviado parece estar vazio.")
            
    # DETETOR INTELIGENTE DE CABEÇALHOS (Agora corta o "lixo" da Shopee)
    linha_cabecalho = 0
    for i in range(min(30, len(df))):
        linha_atual = df.iloc[i].astype(str).str.lower().tolist()
        # ML Ads
        if any('código do anúncio' in v or 'número do anúncio vendido' in v for v in linha_atual):
            linha_cabecalho = i; break
        # ML Desempenho
        elif any('id do anúncio' in v for v in linha_atual):
            linha_cabecalho = i; break
        # SHOPEE Ads (Procura colunas típicas para ignorar as 6 linhas iniciais)
        elif any('nome do anúncio' in v and 'tipos de anúncios' in v for v in linha_atual):
            linha_cabecalho = i; break
        # SHOPEE Desempenho
        elif any('id do item' in v for v in linha_atual):
            linha_cabecalho = i; break
            
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
            return jsonify({"erro": "A planilha de Desempenho (Orgânica) é obrigatória."}), 400

        df_desempenho = carregar_planilha_segura(arq_desempenho, False)
        
        # =========================================================================
        # OMNICHANNEL DETECTOR: Identifica a plataforma pelas colunas
        # =========================================================================
        colunas_des = [c.lower() for c in df_desempenho.columns]
        plataforma = 'Shopee' if 'id do item' in colunas_des else 'Mercado Livre'

        if plataforma == 'Shopee':
            col_id_des = next((c for c in df_desempenho.columns if 'id do item' in c.lower()), 'ID do Item')
            # Prioriza Pedido Pago, se não houver, vai para Pedido Realizado
            col_vendas_brutas = next((c for c in df_desempenho.columns if 'vendas (pedido pago)' in c.lower()), 
                                next((c for c in df_desempenho.columns if 'vendas' in c.lower()), 'Vendas'))
            col_unidades = next((c for c in df_desempenho.columns if 'unidades (pedido pago)' in c.lower()), 
                           next((c for c in df_desempenho.columns if 'unidades' in c.lower()), 'Unidades'))
            col_titulo_des = next((c for c in df_desempenho.columns if 'produto' in c.lower()), 'Produto')
        else:
            col_id_des = next((c for c in df_desempenho.columns if 'id do anúncio' in c.lower()), 'ID do anúncio')
            col_vendas_brutas = next((c for c in df_desempenho.columns if 'vendas brutas' in c.lower() or 'receita' in c.lower()), 'Vendas brutas (BRL)')
            col_unidades = next((c for c in df_desempenho.columns if 'unidades' in c.lower() and 'vendidas' in c.lower()), 'Unidades vendidas')
            col_titulo_des = next((c for c in df_desempenho.columns if 'título' in c.lower() or 'anúncio' in c.lower() and 'id' not in c.lower()), 'Anúncio')

        # Filtra apenas linhas com IDs válidos
        df_desempenho = df_desempenho[df_desempenho[col_id_des].astype(str).str.contains(r'\d', regex=True, na=False)]

        if col_vendas_brutas in df_desempenho.columns:
            df_desempenho[col_vendas_brutas] = df_desempenho[col_vendas_brutas].apply(limpar_moeda)
        
        if col_unidades in df_desempenho.columns:
            df_desempenho[col_unidades] = pd.to_numeric(df_desempenho[col_unidades], errors='coerce').fillna(0)
        else:
            df_desempenho[col_unidades] = 0

        # Tratamento Universal de ID (Remove MLB se existir, limpa espaços)
        df_desempenho['ID_Tratado'] = df_desempenho[col_id_des].astype(str).str.upper().str.replace('MLB', '', regex=False).str.replace(r'\.0$', '', regex=True).str.strip()
        df_desempenho['Anúncio_Clean'] = df_desempenho.get(col_titulo_des, 'Produto sem título').fillna('Produto sem título')
        
        df_desempenho_agrupado = df_desempenho.groupby('ID_Tratado').agg({
            'Anúncio_Clean': 'first', 
            col_vendas_brutas: 'sum',
            col_unidades: 'sum'
        }).reset_index()
        
        df_desempenho_agrupado.rename(columns={'Anúncio_Clean': 'Anúncio'}, inplace=True)
        
        # Ordem e Curva ABC baseada em Unidades (Volume/Giro) - Mantendo a sua última estratégia de ouro!
        df_desempenho_agrupado = df_desempenho_agrupado.sort_values(by=col_unidades, ascending=False).copy()
        
        faturamento_total = round(float(df_desempenho_agrupado[col_vendas_brutas].sum()), 2)
        unidades_total = int(df_desempenho_agrupado[col_unidades].sum())
        
        if unidades_total > 0:
            df_desempenho_agrupado['Percentual_Acumulado'] = (df_desempenho_agrupado[col_unidades].cumsum() / unidades_total) * 100
        else:
            df_desempenho_agrupado['Percentual_Acumulado'] = 0
            
        condicoes = [(df_desempenho_agrupado['Percentual_Acumulado'] <= 80), (df_desempenho_agrupado['Percentual_Acumulado'] > 80) & (df_desempenho_agrupado['Percentual_Acumulado'] <= 95)]
        df_desempenho_agrupado['Curva_ABC'] = np.select(condicoes, ['A', 'B'], default='C')

        has_ads = False
        oportunidades = []
        gargalos = []
        receita_ads_total = 0.0
        investimento_ads_total = 0.0

        if arq_ads:
            has_ads = True
            df_ads = carregar_planilha_segura(arq_ads, True)
            
            # Mapeamento Condicional de Ads (ML vs Shopee)
            if plataforma == 'Shopee':
                col_id_ads = next((c for c in df_ads.columns if 'id do produto' in c.lower()), None)
                col_receita_ads = next((c for c in df_ads.columns if 'gmv' in c.lower()), 'GMV')
                col_invest_ads = next((c for c in df_ads.columns if 'despesas' in c.lower()), 'Despesas')
                col_titulo_ads = next((c for c in df_ads.columns if 'nome do anúncio' in c.lower()), 'Nome do Anúncio')
            else:
                col_id_ads = next((c for c in df_ads.columns if 'código do anúncio' in c.lower() or 'número do anúncio vendido' in c.lower()), None)
                col_receita_ads = next((c for c in df_ads.columns if 'receita' in c.lower() and 'moeda local' in c.lower() and 'diretas' not in c.lower()), 'Receita')
                col_invest_ads = next((c for c in df_ads.columns if 'investimento' in c.lower() and 'moeda local' in c.lower()), None)
                col_titulo_ads = next((c for c in df_ads.columns if 'título' in c.lower() and 'anúncio' in c.lower()), 'Título_Ads')

            if not col_id_ads:
                return jsonify({"erro": f"Não foi possível encontrar a coluna de ID na planilha de Ads da {plataforma}."}), 400

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

            df_final = pd.merge(df_desempenho_agrupado, df_ads_agrupado, on='ID_Tratado', how='outer')
            
            df_final['Anúncio'] = df_final['Anúncio'].fillna(df_final['Anuncio_Ads_Temp']).fillna('Produto apenas em Ads')
            df_final['Curva_ABC'] = df_final['Curva_ABC'].fillna('C')
            df_final[col_vendas_brutas] = df_final[col_vendas_brutas].fillna(0.0)
            df_final[col_unidades] = df_final[col_unidades].fillna(0)
            df_final['Receita_Ads'] = df_final['Receita_Ads'].fillna(0.0)
            df_final['Investimento_Ads'] = df_final.get('Investimento_Ads', 0.0).fillna(0.0)
            
            df_final['Dependencia_Ads'] = np.where(
                df_final[col_vendas_brutas] > 0, 
                (df_final['Receita_Ads'] / df_final[col_vendas_brutas]) * 100, 
                np.where(df_final['Receita_Ads'] > 0, 100, 0)
            )
            df_final['Dependencia_Ads'] = np.minimum(df_final['Dependencia_Ads'], 100) 

            df_final['Alerta_Oportunidade'] = (df_final['Curva_ABC'] == 'A') & (df_final['Receita_Ads'] == 0)
            df_final['Alerta_Gargalo'] = (df_final['Curva_ABC'] == 'C') & (df_final['Investimento_Ads'] > 0) & (df_final[col_vendas_brutas] <= df_final['Investimento_Ads'])

            df_final = df_final.replace([np.inf, -np.inf], 0).fillna(0)

            df_final[col_vendas_brutas] = df_final[col_vendas_brutas].round(2)
            df_final['Receita_Ads'] = df_final['Receita_Ads'].round(2)
            df_final['Investimento_Ads'] = df_final['Investimento_Ads'].round(2)
            df_final['Dependencia_Ads'] = df_final['Dependencia_Ads'].round(2)

            oportunidades = df_final[df_final['Alerta_Oportunidade']][['ID_Tratado', 'Anúncio', col_unidades, col_vendas_brutas]].rename(columns={col_vendas_brutas: 'Faturamento', col_unidades: 'Unidades'}).to_dict('records')
            gargalos = df_final[df_final['Alerta_Gargalo']][['ID_Tratado', 'Anúncio', col_unidades, col_vendas_brutas, 'Receita_Ads', 'Investimento_Ads', 'Dependencia_Ads']].rename(columns={col_vendas_brutas: 'Faturamento', col_unidades: 'Unidades'}).sort_values(by='Investimento_Ads', ascending=False).to_dict('records')
            
            receita_ads_total = round(float(df_final['Receita_Ads'].sum()), 2)
            investimento_ads_total = round(float(df_final['Investimento_Ads'].sum()), 2)
            
            visao_geral = df_final.sort_values(by=col_unidades, ascending=False)[['ID_Tratado', 'Anúncio', 'Curva_ABC', col_unidades, col_vendas_brutas, 'Receita_Ads', 'Investimento_Ads', 'Dependencia_Ads']].rename(columns={col_vendas_brutas: 'Faturamento', col_unidades: 'Unidades'}).to_dict('records')
        
        else:
            df_final = df_desempenho_agrupado.copy()
            df_final['Receita_Ads'] = 0.0
            df_final['Investimento_Ads'] = 0.0
            df_final['Dependencia_Ads'] = 0.0
            
            df_final[col_vendas_brutas] = df_final[col_vendas_brutas].round(2)
            
            visao_geral = df_final.sort_values(by=col_unidades, ascending=False)[['ID_Tratado', 'Anúncio', 'Curva_ABC', col_unidades, col_vendas_brutas, 'Receita_Ads', 'Investimento_Ads', 'Dependencia_Ads']].rename(columns={col_vendas_brutas: 'Faturamento', col_unidades: 'Unidades'}).to_dict('records')

        return jsonify({
            "plataforma": plataforma, # O BACKEND AGORA AVISA O FRONTEND QUAL É A PLATAFORMA!
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
        print(traceback.format_exc())
        return jsonify({"erro": f"Erro na formatação dos dados. Detalhe: {str(e)}"}), 500
