# Stock Manager

Voorraad- en werfbeheer voor een kleine elektrische zaak.

## Lokale start

1. Open PowerShell in deze map.
2. Installeer de afhankelijkheden:
   `pip install -r requirements.txt`
3. Start de app:
   `python app.py`
4. Open:
   `http://127.0.0.1:5000`

De live database staat standaard in `stock_manager.db`.

## Beveiliging

De app ondersteunt een login voor internetgebruik.

Gebruik deze omgevingsvariabelen:

- `STOCK_MANAGER_SECRET_KEY`
- `STOCK_MANAGER_ADMIN_USERNAME`
- `STOCK_MANAGER_ADMIN_PASSWORD`
- `STOCK_MANAGER_DB`
- `STOCK_MANAGER_BACKUP_DIR` (optioneel)
- `STOCK_MANAGER_COOKIE_SECURE`

Als `STOCK_MANAGER_ADMIN_PASSWORD` leeg is, draait de app zonder login.
Als je de app online zet, moet je altijd een sterk wachtwoord en een sterke secret key instellen.

Een voorbeeldbestand staat in `.env.example`.

## Back-ups

- Back-ups maak je via de pagina `Instellingen`
- Back-upbestanden worden standaard opgeslagen in een map `backups` naast de actieve database
- Op Railway is dat normaal `/app/data/backups`

## Veilig testen

Je kunt de app starten met een aparte database:

```powershell
$env:STOCK_MANAGER_DB='test_stock_manager.db'
python app.py
```

## Online zetten op Railway

Dit is nu de aanbevolen manier om de app overal bereikbaar te maken.

### 1. Zet de code in GitHub

Maak een repository aan en push deze map naar GitHub.

### 2. Maak een project aan op Railway

- Kies `New Project`
- Kies `Deploy from GitHub repo`
- Verbind je GitHub repository

Railway leest de meegeleverde `railway.json` en start de app met:

- `gunicorn app:app`

### 3. Voeg een Volume toe

Omdat deze versie SQLite gebruikt, heb je een schijf nodig die bewaard blijft.

Maak in Railway een `Volume` aan en mount die bijvoorbeeld op:

- `/app/data`

Zet daarna deze environment variable:

- `STOCK_MANAGER_DB=/app/data/stock_manager.db`

### 4. Zet deze environment variables

- `STOCK_MANAGER_SECRET_KEY`
- `STOCK_MANAGER_ADMIN_USERNAME`
- `STOCK_MANAGER_ADMIN_PASSWORD`
- `STOCK_MANAGER_DB=/app/data/stock_manager.db`
- `STOCK_MANAGER_BACKUP_DIR=/app/data/backups` (optioneel, maar duidelijk)
- `STOCK_MANAGER_COOKIE_SECURE=1`

Gebruik voor `STOCK_MANAGER_SECRET_KEY` een lange willekeurige tekst.

### 5. Deploy

Na deployment open je de Railway-URL in de browser en log je in met je ingestelde gebruiker en wachtwoord.

### Meegeleverde Railway-bestanden

- `railway.json`
- `Procfile`
- `.env.example`

## Belangrijke opmerking over de database

Deze versie is nu goed genoeg om online te draaien voor persoonlijk gebruik met login en back-ups.

De volgende grotere technische stap voor verdere groei is:

- migratie van SQLite naar PostgreSQL

Dat wordt vooral belangrijk als je later:

- meerdere gebruikers wilt
- hogere betrouwbaarheid wilt
- betere hosting-flexibiliteit wilt
- minder risico op bestandsproblemen wilt

## CSV-import

Op de pagina `Producten` kun je CSV-bestanden importeren.

Benodigde kolommen:

- `article_number`
- `description`
- `unit`
- `purchase_quantity`
- `purchase_price`
- `stock_quantity`
- `profit_margin`
- `category`
- `meter_tracking_enabled`
