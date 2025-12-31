from google import genai
import pandas as pd
import json
import os
from dotenv import load_dotenv


# --- CONFIGURAÇÃO ---
load_dotenv()
API_KEY = os.getenv("API_KEY")
client = genai.Client(api_key=API_KEY)
MODEL_ID = "gemini-2.5-flash" 

def score_and_rank():
    if not os.path.exists("daily_raw_news.csv"): 
        print("Erro: daily_raw_news.csv não encontrado.")
        return
        
    df = pd.read_csv("daily_raw_news.csv")

    # --- ÚNICA ALTERAÇÃO: Removendo Valor Econômico antes do processamento ---
    df = df[df['source'] != "Valor Econômico"].reset_index(drop=True)

    if df.empty:
        print("Nenhuma notícia restante após o filtro.")
        return

    headlines_text = "\n".join([f"ID:{i} | {row['source']} | {row['title']}" for i, row in df.iterrows()])

    # Prompt Calibrado: Foco em "Mudança de Fundamento"
    prompt = f"""
    Atue como um Diretor de Investimentos (CIO). Sua missão é classificar notícias pelo valor de leitura para profissionais de finanças (IB, Asset, Wealth, Corporate Finance).
    RUBRICA DE PONTUAÇÃO (1-10):

    1. MUDANÇA DE FUNDAMENTO (Nota 9-10):
       - GOVERNANÇA: Troca de CEO/CFO, disputas societárias ou mudanças no conselho.
       - ESTRATÉGIA: Pivôs de modelo de negócio, encerramento de operações grandes, ou novos produtos disruptivos.
       - REGULATÓRIO/JURÍDICO: Vitórias judiciais tributárias, mudanças em concessões ou novas normas setoriais (CVM/BC).
       - CAPITAL: Dividendos extraordinários, reestruturações de dívida ou M&As transformacionais.
       - MACRO SISTÊMICO: Mudanças severas em juros ou política fiscal.

    2. DINÂMICA DE MERCADO (Nota 7-8):
       - Resultados trimestrais com surpresas (positivas ou negativas) em margens e EBITDA.
       - MACRO ESTRUTURANTE: Juros (Fed/Copom) ou mudanças fiscais que alteram o custo de capital (WACC).
       - CAPTAÇÕES: M&As de médio porte e emissões de dívida.

    3. CONTEXTO E SETORIAL (Nota 4-6):
       - Tendências de mercado, movimentações de concorrentes menores e dados de consumo.
       - Volatilidade política ou geopolítica que não altera o fluxo de caixa direto.

    4. RUÍDO (Nota 1-3):
       - Variações diárias de preços ("Ação X sobe 2%"), marketing e retórica sem ação prática.

    DETERMINAÇÃO: Não privilegie M&A sobre Estratégia, Governança ou Regulatório. O critério de desempate é: "Qual notícia mudaria mais a projeção de fluxo de caixa em um modelo de 5 anos?".

    Retorne APENAS um JSON: [{{"id": int, "score": int}}]
    
    MANCHETES:
    {headlines_text}
    """

    try:
        print(f"Analisando sinais (Foco em Fundamentos e Governança)...")
        
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=prompt
        )
        
        raw_output = response.text.replace('```json', '').replace('```', '').strip()
        results = json.loads(raw_output)
        
        results_df = pd.DataFrame(results).set_index('id')
        final_df = df.join(results_df)

        # --- ALTERAÇÃO: Bônus Elite apenas para Brazil Journal ---
        elite_sources = ["Brazil Journal"]
        final_df['score'] = final_df.apply(
            lambda x: min(10.0, float(x['score']) + 1) if x['source'] in elite_sources else float(x['score']), 
            axis=1
        )

        ranked = final_df.sort_values(by='score', ascending=False)
        ranked.to_csv("daily_ranked_news.csv", index=False)
        
        print(f"\n{'='*75}\n RADAR ESTRATÉGICO: FUNDAMENTOS, GOVERNANÇA E MACRO\n{'='*75}")
        for _, row in ranked.head(10).iterrows():
            print(f"[{row['score']:.1f}/10] {row['source']}")
            print(f"SINAL: {row['title']}\n")

    except Exception as e:
        print(f"Erro na análise: {e}")

if __name__ == "__main__":
    score_and_rank()