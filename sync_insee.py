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

def download_and_extract_csv(url):
    print(f"Téléchargement de {url}...")
    try:
        response = requests.get(url, timeout=300)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            print(f"Extraction de {csv_filename}...")
            with z.open(csv_filename) as csv_file:
                content = csv_file.read().decode('latin-1')
                return content
    except Exception as e:
        print(f"Erreur: {e}")
        return None

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
            supabase.table('insee_deces').insert(batch).execute()
            print(f"Batch {i//batch_size + 1}/{(total + batch_size - 1)//batch_size} inséré")
        except Exception as e:
            print(f"Erreur: {e}")

def main():
    print("Démarrage de la synchronisation INSEE complète...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Download all years from 1975 to 2025
    for year in range(1975, 2026):
        print(f"\n=== Année {year} ===")

        # Try annual file first
        url = f"{INSEE_BASE_URL}/Deces_{year}.zip"
        csv_content = download_and_extract_csv(url)

        if csv_content:
            records = parse_insee_csv(csv_content)
            print(f"{len(records)} enregistrements trouvés pour {year}")
            batch_insert_to_supabase(supabase, records)
        else:
            # If annual file doesn't exist, try monthly files
            print(f"Fichier annuel non trouvé, essai des fichiers mensuels...")
            for month in range(1, 13):
                url = f"{INSEE_BASE_URL}/Deces_{year}_M{month:02d}.zip"
                csv_content = download_and_extract_csv(url)

                if csv_content:
                    records = parse_insee_csv(csv_content)
                    print(f"{len(records)} enregistrements trouvés pour {year}-{month:02d}")
                    batch_insert_to_supabase(supabase, records)

    print("\nSynchronisation terminée!")

if __name__ == "__main__":
    main()
