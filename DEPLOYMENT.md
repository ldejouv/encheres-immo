# Deploiement sur Streamlit Cloud

## Prerequis

- Un compte GitHub avec le repository `encheres-immo` pousse
- Un compte Streamlit Cloud (gratuit) : https://share.streamlit.io

## Etapes de deploiement

### 1. Pousser le code sur GitHub

Si ce n'est pas deja fait, poussez votre repository sur GitHub :

```bash
git remote add github https://github.com/VOTRE_UTILISATEUR/encheres-immo.git
git push github main
```

### 2. Connecter Streamlit Cloud

1. Allez sur https://share.streamlit.io
2. Connectez-vous avec votre compte GitHub
3. Cliquez sur **"New app"**

### 3. Configurer l'application

Remplissez les champs suivants :

| Champ | Valeur |
|-------|--------|
| **Repository** | `VOTRE_UTILISATEUR/encheres-immo` |
| **Branch** | `main` |
| **Main file path** | `dashboard/app.py` |

### 4. Deployer

Cliquez sur **"Deploy!"**. Streamlit Cloud va :

1. Cloner votre repository
2. Installer les dependances depuis `requirements.txt`
3. Lancer `dashboard/app.py`
4. Vous fournir une URL publique du type : `https://votre-app.streamlit.app`

## Acceder a l'application

Une fois deployee, votre application est accessible a l'URL :

```
https://VOTRE_APP.streamlit.app
```

Cette URL est partageable avec d'autres personnes.

## Limitations importantes

### Base de donnees ephemere

Streamlit Cloud utilise un systeme de fichiers **ephemere**. Cela signifie que :

- La base SQLite est recree a chaque redemarrage de l'application
- Les donnees scrapees ne persistent pas entre les redemarrages
- L'application demarre avec une base vide (les tables sont creees automatiquement)

Pour une utilisation en production avec persistance des donnees, il faudrait :

- Migrer vers une base PostgreSQL hebergee (ex: Supabase, Neon, ElephantSQL)
- Ou utiliser un stockage externe (ex: Google Sheets, S3)

### Scraping

Le scraper peut fonctionner depuis Streamlit Cloud via la page "Administration", mais :

- Les donnees scrapees seront perdues au prochain redemarrage
- Les IPs de Streamlit Cloud peuvent etre bloquees par le site cible

## Mise a jour

Chaque `git push` sur la branche configuree declenche automatiquement un redeploiement.

## Variables d'environnement (optionnel)

Si vous ajoutez des variables d'environnement plus tard (cles API, connexion DB externe, etc.), vous pouvez les configurer dans :

**Streamlit Cloud** > **Settings** > **Secrets**

Format du fichier secrets (TOML) :

```toml
[database]
url = "postgresql://user:pass@host:5432/dbname"
```

Accessible dans le code avec `st.secrets["database"]["url"]`.
