# INSEE Database Sync

Synchronisation automatique de la base de données des décès de l'INSEE vers Supabase.

## Configuration

1. Ajouter les secrets GitHub (Settings > Secrets and variables > Actions):
   - `SUPABASE_URL`: https://ouhqmwlrombktjkzmzsd.supabase.co
   - `SUPABASE_KEY`: Votre clé API Supabase

2. Le script s'exécute automatiquement tous les jours à 00h (heure de Paris)

3. Vous pouvez aussi lancer manuellement via l'onglet "Actions" sur GitHub

## Structure de la table Supabase

La table `insee_deces` contient :
- nom
- prenoms
- sexe
- date_naissance
- code_lieu_naissance
- lieu_naissance
- date_deces
- code_lieu_deces
- numero_acte
