import pandas as pd
from google import genai
import requests
import trafilatura
import os
import time
from dotenv import load_dotenv


# --- CONFIGURAÇÃO ---
load_dotenv()
API_KEY = os.getenv("API_KEY")
client = genai.Client(api_key=API_KEY)
MODEL_ID = "gemini-2.5-flash" # Use o 2.0 Flash para maior velocidade e precisão no tom

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
}

def resolve_google_link(url):
    try:
        response = requests.get(url, headers=HEADERS, allow_redirects=True, timeout=15)
        return response.url
    except:
        return url

def get_content_via_jina(url):
    jina_url = f"https://r.jina.ai/{url}"
    try:
        response = requests.get(jina_url, timeout=25)
        if response.status_code == 200 and len(response.text) > 600:
            return response.text
    except:
        pass
    return None

def generate_briefing():
    if not os.path.exists("daily_ranked_news.csv"):
        print("Erro: daily_ranked_news.csv não encontrado.")
        return

    df = pd.read_csv("daily_ranked_news.csv").head(5)
    print(f"Iniciando síntese narrativa mimetizada para {len(df)} notícias...")
    
    full_report = f"# RADAR FINANCEIRO EXECUTIVO - {pd.Timestamp.now(tz='America/Sao_Paulo').strftime('%d/%m/%Y')}\n\n"

    for i, row in df.iterrows():
        print(f"[{i+1}/5] Processando: {row['title'][:40]}...")
        
        real_url = resolve_google_link(row['link'])
        content = get_content_via_jina(real_url)
        
        if not content or len(content) < 600:
            downloaded = trafilatura.fetch_url(real_url)
            content = trafilatura.extract(downloaded) if downloaded else ""

        if not content or len(content) < 400:
            print(f"   ! Falha crítica na extração. Pulando...")
            continue

        # --- NOVO PROMPT: MIMETISMO E DENSIDADE ---
        prompt = f"""
        Atue como um Analista Sênior de uma Asset Management. Produza uma síntese narrativa de altíssima fidelidade.

        OBJETIVO:
        1. MIMETISMO: Adote o estilo de escrita da fonte ({row['source']}). Se for analítico e ácido, mantenha.
        2. MAXIMIZAÇÃO DE DADOS: Preserve nomes de executivos, valores (M&A), múltiplos (EBITDA, P/L), e nuances estratégicas.
        3. FLUIDEZ: Texto em parágrafos coesos. Proibido introduções ou listas.

        REGRAS DE FORMATAÇÃO (CRÍTICO):
        - PROIBIDO o uso de Markdown no corpo do texto (não use '*', '_', ou '**'). 
        - O texto deve ser inteiramente "Plain Text" (texto puro).
        - Nomes de jornais, termos em inglês ou siglas devem ser escritos normalmente, sem itálico ou negrito.
        - Não use aspas para enfatizar termos, a menos que seja uma citação direta.

        CONTEÚDO PARA SÍNTESE:
        {content[:15000]}
        """
        
        try:
            response = client.models.generate_content(model=MODEL_ID, contents=prompt)
            
            full_report += f"### {row['source']}: {row['title']}\n"
            full_report += f"{response.text.strip()}\n"
            full_report += f"\n**Link original:** {real_url}\n\n---\n\n"
            
            time.sleep(1)
        except Exception as e:
            print(f"   ! Erro na API Gemini: {e}")

    with open("EXECUTIVE_BRIEF.md", "w", encoding="utf-8") as f:
        f.write(full_report)
    
    print("\nPROCESSO CONCLUÍDO. O relatório mimetizado está pronto.")

if __name__ == "__main__":
    generate_briefing()