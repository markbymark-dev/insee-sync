#!/usr/bin/env python3
import os
import requests
import zipfile
import io
import csv
import sys
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
    
    failed_batches = 0
    successful_batches = 0
    
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        try:
            result = supabase.table('insee_deces').insert(batch).execute()
            successful_batches += 1
            print(f"Batch {i//batch_size + 1}/{(total + batch_size - 1)//batch_size} inséré avec succès")
        except Exception as e:
            failed_batches += 1
            print(f"ERREUR batch {i//batch_size + 1}: {e}")
            # Continue to try other batches but track failures
    
    if failed_batches > 0:
        error_msg = f"ÉCHEC: {failed_batches} batch(s) ont échoué sur {successful_batches + failed_batches} total"
        print(error_msg)
        raise Exception(error_msg)
    
    print(f"Succès: {successful_batches} batch(s) insérés")

def main():
    print("Démarrage de la synchronisation INSEE (2018-2025)...")
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERREUR: Variables d'environnement SUPABASE_URL ou SUPABASE_KEY manquantes")
        sys.exit(1)
    
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    total_records_inserted = 0
    years_failed = []
    
    # Download years from 2018 to 2025 (INSEE files before 2018 are no longer available)
    for year in range(2018, 2026):
        print(f"\n=== Année {year} ===")
        
        # Try annual file
        url = f"{INSEE_BASE_URL}/Deces_{year}.zip"
        csv_content = download_and_extract_csv(url)
        
        if csv_content:
            try:
                records = parse_insee_csv(csv_content)
                print(f"{len(records)} enregistrements trouvés pour {year}")
                
                if records:
                    batch_insert_to_supabase(supabase, records)
                    total_records_inserted += len(records)
            except Exception as e:
                print(f"ERREUR pour l'année {year}: {e}")
                years_failed.append(year)
        else:
            print(f"Fichier annuel {year} non disponible")
            years_failed.append(year)
    
    print(f"\n=== RÉSUMÉ ===")
    print(f"Total d'enregistrements insérés: {total_records_inserted}")
    
    if years_failed:
        print(f"Années échouées: {years_failed}")
        print(f"ÉCHEC: {len(years_failed)} année(s) n'ont pas pu être synchronisées")
        sys.exit(1)
    else:
        print("Synchronisation terminée avec succès!")

if __name__ == "__main__":
    main()
