import feedparser
import pandas as pd
import requests
import urllib.parse
from datetime import datetime
import pytz

BR_TZ = pytz.timezone('America/Sao_Paulo')

def get_proxy_rss(query, name, limit=12):
    encoded_query = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={encoded_query}+when:24h&hl=pt-BR&gl=BR&ceid=BR:pt-419"
    try:
        resp = requests.get(url, timeout=15)
        feed = feedparser.parse(resp.content)
        articles = [{"source": name, "title": entry.title.split(' - ')[0], "link": entry.link} for entry in feed.entries[:limit]]
        return articles, "Success ✅"
    except: return [], "Failed ❌"

def fetch_direct_rss(name, url, limit=15):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        feed = feedparser.parse(resp.content)
        articles = []
        
        # FILTRO DE RUÍDO EXPANDIDO (Nível Institucional)
        noise = [
            'mega da virada', 'gastronomia', 'viagem', 'hotel', 'restaurante', 'vinho',
            'carreira', 'vagas', 'currículo', 'cartão de crédito', 'milhas', 
            'poupanca', 'fgts', 'irpf', 'imposto de renda', 'como economizar',
            'aprenda a investir', 'guia', 'onde comer', 'melhores destinos'
        ]
        
        for entry in feed.entries:
            if len(articles) >= limit: break
            
            title_lower = entry.title.lower()
            if any(key in title_lower for key in noise): continue
            
            articles.append({"source": name, "title": entry.title, "link": entry.link})
            
        return articles, "Success ✅"
    except: return [], "Failed ❌"

def run_ingestion():
    print(f"--- Iniciando Coleta: {datetime.now(BR_TZ).strftime('%H:%M:%S')} ---")
    all_articles = []
    
    # 1. Fontes Tier 1 (Removido Valor Econômico)
    sources_t1 = [
        ("Brazil Journal", "https://braziljournal.com/feed/")
    ]
    
    for name, target in sources_t1:
        if "site:" in target:
            arts, status = get_proxy_rss(target, name, limit=15)
        else:
            arts, status = fetch_direct_rss(name, target, limit=15)
        all_articles.extend(arts)
        print(f"[{status}] {name:<15}: {len(arts)} artigos")

    # 2. Fontes de Apoio
    sources_t2 = [
        ("NeoFeed", "https://neofeed.com.br/feed/", 10),
        ("InfoMoney", "https://www.infomoney.com.br/mercados/feed/", 3),
        ("Exame Invest", "https://exame.com/invest/feed/", 10)
    ]
    
    for name, url, limit in sources_t2:
        arts, status = fetch_direct_rss(name, url, limit=limit)
        all_articles.extend(arts)
        print(f"[{status}] {name:<15}: {len(arts)} artigos")

    df = pd.DataFrame(all_articles)
    df.to_csv("daily_raw_news.csv", index=False)
    print(f"\nIngestão Completa: {len(df)} artigos salvos.")

if __name__ == "__main__":
    run_ingestion()