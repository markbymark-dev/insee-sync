import os
import requests
import pandas as pd
from supabase import create_client, Client
from datetime import datetime

# Supabase configuration
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def download_insee_file(year):
    """Download INSEE death records file for a given year"""
    # INSEE base URL
    base_url = "https://www.insee.fr/fr/statistiques/fichier/4190491"
    
    # Construct filename based on year
    if year >= 2024:
        # New format: deces-2024.csv
        filename = f"deces-{year}.csv"
    else:
        # Old format: Deces_2023.txt
        filename = f"Deces_{year}.txt"
    
    url = f"{base_url}/{filename}"
    
    print(f"Downloading {filename} from {url}")
    
    try:
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        
        # Save to temporary file
        temp_file = f"/tmp/{filename}"
        with open(temp_file, 'wb') as f:
            f.write(response.content)
        
        print(f"Downloaded {filename} successfully ({len(response.content)} bytes)")
        return temp_file
    except Exception as e:
        print(f"Error downloading {filename}: {str(e)}")
        return None

def parse_insee_file(filepath, year):
    """Parse INSEE file and return DataFrame with standardized columns"""
    print(f"Parsing {filepath}")
    
    try:
        if year >= 2024:
            # New CSV format with different column names
            df = pd.read_csv(filepath, sep=';', dtype=str, encoding='utf-8')
            
            print(f"Columns found: {list(df.columns)}")
            print(f"First row sample: {df.iloc[0].to_dict() if len(df) > 0 else 'No data'}")
            
            # Map new column names to our database schema
            # New format columns: ADEC, ANAIS, COMDEC, DEPDEC, DEPDOM, DEPNAIS, 
            # JDEC, JNAIS, LIEUDEC_R, MDEC, MNAIS, PNAIS, REGDEC, REGDOM, SEXE
            
            df_mapped = pd.DataFrame({
                'sexe': df['SEXE'].map({'1': 'M', '2': 'F'}),
                'date_naissance': df.apply(lambda row: f"{row['ANAIS']}-{str(row['MNAIS']).zfill(2)}-{str(row['JNAIS']).zfill(2)}" 
                                          if pd.notna(row['ANAIS']) and pd.notna(row['MNAIS']) and pd.notna(row['JNAIS']) 
                                          else None, axis=1),
                'date_deces': df.apply(lambda row: f"{row['ADEC']}-{str(row['MDEC']).zfill(2)}-{str(row['JDEC']).zfill(2)}" 
                                      if pd.notna(row['ADEC']) and pd.notna(row['MDEC']) and pd.notna(row['JDEC']) 
                                      else None, axis=1),
                'commune_deces': df['COMDEC'],
                'departement_deces': df['DEPDEC'],
                'departement_domicile': df['DEPDOM'],
                'departement_naissance': df['DEPNAIS'],
                'pays_naissance': df['PNAIS'],
                'lieu_deces': df['LIEUDEC_R'] if 'LIEUDEC_R' in df.columns else None,
                'region_deces': df['REGDEC'],
                'region_domicile': df['REGDOM'],
                'annee': year,
                'nom': None,  # No longer available in new format
                'prenoms': None  # No longer available in new format
            })
            
        else:
            # Old TXT format with fixed-width columns
            df = pd.read_csv(filepath, sep=';', dtype=str, encoding='latin-1')
            
            print(f"Columns found: {list(df.columns)}")
            
            # Map old column names
            df_mapped = pd.DataFrame({
                'nom': df.get('Nom'),
                'prenoms': df.get('Prénoms'),
                'sexe': df.get('Sexe'),
                'date_naissance': df.get('Date naissance'),
                'date_deces': df.get('Date décès'),
                'commune_deces': df.get('Commune décès'),
                'departement_deces': df.get('Département décès'),
                'departement_domicile': df.get('Département domicile'),
                'departement_naissance': df.get('Département naissance'),
                'pays_naissance': df.get('Pays naissance'),
                'lieu_deces': df.get('Lieu décès'),
                'region_deces': df.get('Région décès'),
                'region_domicile': df.get('Région domicile'),
                'annee': year
            })
        
        # Clean up data - replace NaN with empty string
        df_mapped = df_mapped.fillna('')
        
        print(f"Parsed {len(df_mapped)} records from {filepath}")
        print(f"Sample record: {df_mapped.iloc[0].to_dict() if len(df_mapped) > 0 else 'No data'}")
        
        return df_mapped
        
    except Exception as e:
        print(f"Error parsing {filepath}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def insert_to_supabase(df, batch_size=1000):
    """Insert DataFrame records to Supabase in batches"""
    print(f"Inserting {len(df)} records to Supabase")
    
    total_inserted = 0
    
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        records = batch.to_dict('records')
        
        try:
            result = supabase.table('insee_deces').insert(records).execute()
            total_inserted += len(records)
            print(f"Inserted batch {i//batch_size + 1}: {len(records)} records (total: {total_inserted})")
        except Exception as e:
            print(f"Error inserting batch {i//batch_size + 1}: {str(e)}")
            print(f"Sample record from failed batch: {records[0] if records else 'No records'}")
            continue
    
    print(f"Total inserted: {total_inserted} records")
    return total_inserted

def main():
    """Main function to sync INSEE death records"""
    print("Starting INSEE death records sync")
    print(f"Timestamp: {datetime.now()}")
    
    # Years to sync (1975-2025)
    years = range(1975, 2026)
    
    total_records = 0
    
    for year in years:
        print(f"\n{'='*50}")
        print(f"Processing year {year}")
        print(f"{'='*50}")
        
        # Download file
        filepath = download_insee_file(year)
        if not filepath:
            print(f"Skipping year {year} - download failed")
            continue
        
        # Parse file
        df = parse_insee_file(filepath, year)
        if df is None or len(df) == 0:
            print(f"Skipping year {year} - parsing failed or no records")
            continue
        
        # Insert to Supabase
        inserted = insert_to_supabase(df)
        total_records += inserted
        
        # Clean up temp file
        try:
            os.remove(filepath)
        except:
            pass
    
    print(f"\n{'='*50}")
    print(f"Sync completed!")
    print(f"Total records inserted: {total_records}")
    print(f"Timestamp: {datetime.now()}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()
