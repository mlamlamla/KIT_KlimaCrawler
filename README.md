# KIT KlimaCrawler 🌍

Pipeline zur systematischen Erfassung von Energie- und Klimarichtlinien bayerischer Kommunen für die **GraphRAG-Analyse**.

---

## 🛠 Voraussetzungen

- Python 3.9+ (empfohlen: 3.10)
- macOS, Linux oder Windows
- Stabile Internetverbindung
- Ausreichend Laufzeit (Großstädte: > 10 Stunden)

---

## 📦 Setup & Installation

### 1️⃣ Repository klonen

```bash
git clone <repository-url>
cd KIT_KlimaCrawler
```

---

### 2️⃣ Virtuelle Umgebung erstellen (wi2026)

#### Windows (PowerShell)

```powershell
python -m venv wi2026
.\wi2026\Scripts\Activate.ps1
```

#### Windows (CMD)

```cmd
python -m venv wi2026
wi2026\Scripts\activate
```

#### macOS / Linux

```bash
python3 -m venv wi2026
source wi2026/bin/activate
```

#### Alternative: Conda

```bash
conda create -n wi2026 python=3.10 -y
conda activate wi2026
```

---

### 3️⃣ Abhängigkeiten installieren

```bash
pip install -r requirements.txt
```

---

## 👯 Verteiltes Arbeiten (Sharding)

Um Bayern parallel zu crawlen, werden vorab aufgeteilte Datenbank-Pakete verwendet.

> ⚠️ **WICHTIG:**  
> In jedem Paket-Ordner heißt die Datei identisch: `crawl.sqlite`.  
> Der Dateiname muss exakt so bleiben, da der Code darauf referenziert.  
> Der Inhalt ist jedoch pro Paket einzigartig (unterschiedliche Kommunen).

### Ablauf

1. **Reservieren**  
   Trage deinen Namen im Repository in die Datei `TRACKER.md` beim gewählten Paket ein (z. B. `pkg_05`) und pushe die Änderung.

2. **Paket laden**  
   Lade den entsprechenden Ordner aus der Cloud/GitHub herunter.

3. **Platzieren**  
   Kopiere die enthaltene `crawl.sqlite` in:

   ```
   crawler/data/db/
   ```

   (Bestehende Datei überschreiben.)

4. **Starten (ohne Limit)**  

   ```bash
   caffeinate -i python3 -m crawler.scripts.run_worker
   ```

5. **Upload nach Abschluss**  
   Datei umbenennen in:

   ```
   pkg_XX_DONE_Name.sqlite
   ```

   und wieder hochladen.

---

## 🏃‍♂️ Crawl starten (Allgemein)

Falls unabhängig von Paketen gearbeitet wird, kann der Umfang manuell gesteuert werden.

### Einzel-Lauf (eine Kommune)

```bash
python3 -m crawler.scripts.run_worker --limit 1
```

### Batch-Lauf (z. B. 100 Kommunen)

```bash
python3 -m crawler.scripts.run_worker --limit 100
```

---

## ☕ WICHTIG: Standby verhindern

Wenn der Rechner in den Ruhezustand geht, bricht die Netzwerkverbindung ab und der Crawl stoppt.

### macOS

```bash
caffeinate -i python3 -m crawler.scripts.run_worker
```

### Windows

Nutze Tools wie:

- Caffeine  
- PowerToys Awake  

---

## 📊 Monitoring & Erfolgskontrolle

### Fortschritt prüfen (Extraktions-Statistik)

```bash
sqlite3 crawler/data/db/crawl.sqlite "SELECT segment_type, COUNT(*) FROM segments GROUP BY segment_type;"
```

### Erfolgreicher Lauf

- `run_worker` beendet sich ohne Fehlermeldung  
- `seed_jobs.status` steht auf `done`  
- Tabellen `documents_raw` und `segments` sind befüllt  

---

## ⚠ Fehlerquellen

- **Netzwerk:** VPN-Abbruch oder instabiles WLAN beendet den Prozess  
- **Tools:** `pdftotext` muss im Systempfad installiert sein  
- **Pfad-Fehler:** Die Datenbank muss exakt in `crawler/data/db/` liegen  

---

## 📌 Kurzfassung (TL;DR)

```bash
git clone <repository-url>
cd KIT_KlimaCrawler
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
caffeinate -i python3 -m crawler.scripts.run_worker
```