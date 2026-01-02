import feedparser
import pandas as pd
import requests
import urllib.parse
from datetime import datetime
import pytz
import os
import json

BR_TZ = pytz.timezone('America/Sao_Paulo')
HISTORY_FILE = "news_history.json"

def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except:
            return set()
    return set()

def save_history(new_urls):
    current_history = load_history()
    updated_history = list(current_history.union(set(new_urls)))
    
    if len(updated_history) > 1500:
        updated_history = updated_history[-1500:]
        
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(updated_history, f)

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
        noise = [
            'mega da virada', 'gastronomia', 'viagem', 'hotel', 'restaurante', 'vinho',
            'carreira', 'vagas', 'horóscopo', 'futebol', 'bbb', 'reality', 'quiz'
        ]
        for entry in feed.entries[:limit]:
            title_lower = entry.title.lower()
            if any(key in title_lower for key in noise): continue
            articles.append({"source": name, "title": entry.title, "link": entry.link})
        return articles, "Success ✅"
    except: return [], "Failed ❌"

def run_ingestion():
    print(f"--- Iniciando Coleta: {datetime.now(BR_TZ).strftime('%H:%M:%S')} ---")
    seen_links = load_history()
    all_articles = []
    new_links_to_save = []
    
    sources_t1 = [("Brazil Journal", "https://braziljournal.com/feed/")]
    
    for name, target in sources_t1:
        if "site:" in target: arts, status = get_proxy_rss(target, name, limit=15)
        else: arts, status = fetch_direct_rss(name, target, limit=15)
            
        count_new = 0
        for art in arts:
            if art['link'] not in seen_links:
                all_articles.append(art)
                new_links_to_save.append(art['link'])
                seen_links.add(art['link'])
                count_new += 1
        print(f"[{status}] {name:<15}: {len(arts)} lidos | {count_new} NOVOS")

    sources_t2 = [
        ("NeoFeed", "https://neofeed.com.br/feed/", 10),
        ("InfoMoney", "https://www.infomoney.com.br/mercados/feed/", 5),
        ("Exame Invest", "https://exame.com/invest/feed/", 10)
    ]
    
    for name, target, limit in sources_t2:
        arts, status = fetch_direct_rss(name, target, limit=limit)
        count_new = 0
        for art in arts:
            if art['link'] not in seen_links:
                all_articles.append(art)
                new_links_to_save.append(art['link'])
                seen_links.add(art['link'])
                count_new += 1
        print(f"[{status}] {name:<15}: {len(arts)} lidos | {count_new} NOVOS")

    if all_articles:
        df = pd.DataFrame(all_articles)
        df = df.drop_duplicates(subset=['link'])
        df.to_csv("daily_raw_news.csv", index=False)
        print(f"Total de notícias inéditas salvas: {len(df)}")
        save_history(new_links_to_save)
    else:
        print("Nenhuma notícia nova encontrada em relação ao histórico.")
        pd.DataFrame(columns=['source', 'title', 'link']).to_csv("daily_raw_news.csv", index=False)

if __name__ == "__main__":
    run_ingestion()
