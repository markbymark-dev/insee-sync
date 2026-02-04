#!/usr/bin/env python3
import os
import requests
import zipfile
import io
import csv
from datetime import datetime
from supabase import create_client, Client

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
INSEE_BASE_URL = "https://www.insee.fr/fr/statistiques/fichier/4190491"

def get_current_month_file():
    now = datetime.now()
    year = now.year
    month = now.month
    return f"{INSEE_BASE_URL}/Deces_{year}_M{month:02d}.zip"

def download_and_extract_csv(url):
    print(f"Téléchargement de {url}...")
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        csv_filename = z.namelist()[0]
        print(f"Extraction de {csv_filename}...")
        with z.open(csv_filename) as csv_file:
            content = csv_file.read().decode('utf-8')
            return content

def parse_insee_csv(csv_content):
    reader = csv.DictReader(io.StringIO(csv_content), delimiter=';')
    records = []
    for row in reader:
        date_naissance = None
        if row.get('Date naissance'):
            try:
                date_naissance = datetime.strptime(row['Date naissance'], '%Y%m%d').date().isoformat()
            except:
                pass
        date_deces = None
        if row.get('Date décès'):
            try:
                date_deces = datetime.strptime(row['Date décès'], '%Y%m%d').date().isoformat()
            except:
                pass
        record = {
            'nom': row.get('Nom', '').strip(),
            'prenoms': row.get('Prénoms', '').strip(),
            'sexe': row.get('Sexe', '').strip(),
            'date_naissance': date_naissance,
            'code_lieu_naissance': row.get('Code lieu naissance', '').strip(),
            'lieu_naissance': row.get('Lieu naissance', '').strip(),
            'date_deces': date_deces,
            'code_lieu_deces': row.get('Code lieu décès', '').strip(),
            'numero_acte': row.get('Numéro acte', '').strip()
        }
        records.append(record)
    return records

def batch_insert_to_supabase(supabase: Client, records, batch_size=1000):
    total = len(records)
    print(f"Insertion de {total} enregistrements...")
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        try:
            supabase.table('insee_deces').upsert(batch, on_conflict='nom,prenoms,date_naissance,date_deces,numero_acte').execute()
            print(f"Batch {i//batch_size + 1}/{(total + batch_size - 1)//batch_size} inséré")
        except Exception as e:
            print(f"Erreur: {e}")

def main():
    print("Démarrage de la synchronisation INSEE...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    url = get_current_month_file()
    csv_content = download_and_extract_csv(url)
    records = parse_insee_csv(csv_content)
    print(f"{len(records)} enregistrements trouvés")
    batch_insert_to_supabase(supabase, records)
    print("Synchronisation terminée!")

if __name__ == "__main__":
    main()
