import subprocess
import json
import os
from datetime import datetime
import pytz

def run_pipeline():
    # 1. Executa os scripts em ordem
    scripts = ["ingestor.py", "analyst.py", "briefer.py"]
    
    for script in scripts:
        if os.path.exists(script):
            print(f"Rodando {script}...")
            subprocess.run(["python", script], check=True)
        else:
            print(f"Erro: {script} não encontrado.")
            return

    # 2. Transforma o Markdown gerado em JSON para o site
    if os.path.exists("EXECUTIVE_BRIEF.md"):
        with open("EXECUTIVE_BRIEF.md", "r", encoding="utf-8") as f:
            content = f.read()
        
        data = {
            "last_update": datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%d/%m/%Y %H:%M'),
            "content_md": content
        }
        
        with open("web_data.json", "w", encoding="utf-8") as j:
            json.dump(data, j, ensure_ascii=False, indent=4)
        
        print("Sucesso: web_data.json atualizado com as notícias de hoje.")

if __name__ == "__main__":
    run_pipeline()