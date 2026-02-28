# KIT KlimaCrawler 🌍

---

## 🛠 Voraussetzungen

- Python **3.9+** (empfohlen: 3.10)
- macOS, Linux oder Windows
- Stabile Internetverbindung
- Ausreichend Laufzeit (große Kommunen > 10h)

---

## 📦 Setup & Installation

### 1. Repository klonen

```bash
git clone <repository-url>
cd KIT_KlimaCrawler
```

---

### 2. Virtuelle Umgebung erstellen (empfohlen)

Erstellt einen lokalen Ordner namens **wi2026** in deinem Projektverzeichnis.

---

## 🪟 Windows

### PowerShell
# Erstellen
python -m venv wi2026

# Aktivieren
.\wi2026\Scripts\Activate.ps1

# CMD
# Erstellen
python -m venv wi2026

# Aktivieren
wi2026\Scripts\activate

## 🍎 macOS / 🐧 Linux

# Bash
# Erstellen
python3 -m venv wi2026

# Aktivieren
source wi2026/bin/activate

Option 2: Anaconda / Miniconda (conda)

Verwaltet virtuelle Umgebungen zentral im System.

# Umgebung erstellen
conda create -n wi2026 python=3.11

# Aktivieren
conda activate wi2026

---

### 3. Abhängigkeiten installieren

```bash
pip install -r requirements.txt
```

---

## 🏃‍♂️ Crawl starten

Der Crawler arbeitet eine SQLite-Datenbank als Job-Queue ab (`seed_jobs`).

### Standard-Start (eine Kommune)

```bash
python3 -m crawler.scripts.run_worker --limit 1
```

- `--limit 1` = verarbeitet eine Kommune
- Erhöhen für Batch-Verarbeitung mehrerer Kommunen (limit 100)

---

## ☕ WICHTIG: Standby verhindern

Tiefe Crawls können **mehrere Stunden** dauern.
Geht der Rechner in den Standby, **stoppt der Crawl**.

### macOS (empfohlen)

```bash
caffeinate -i python3 -m crawler.scripts.run_worker --limit 1
```

Der Mac bleibt exakt so lange wach, wie der Crawl läuft.

Alternative: App **Amphetamine**

---

### Windows

Nutze eines der folgenden Tools:

- **Caffeine**
- **PowerToys → Awake**

---

## 📊 Crawl überwachen (optional)

Live-Check der extrahierten Dokumentsegmente:

```bash
sqlite3 crawler/data/db/crawl.sqlite \
"SELECT segment_type, COUNT(*) FROM segments GROUP BY segment_type;"
```

---

## ✅ Erfolgreicher Lauf

Ein erfolgreicher Crawl bedeutet:

- `run_worker` beendet sich ohne Fehler
- Neue Einträge in `crawl.sqlite`
- PDFs und HTML-Segmente wurden extrahiert

---

## ⚠ Typische Fehlerquellen

- Rechner geht in Standby
- VPN/Netzwerkabbruch
- Fehlende PDF-Tools (pdftotext nicht installiert)
- Abbruch durch manuelles Schließen des Terminals

---

## 📌 Kurzfassung (TL;DR)

```bash
git clone <repository-url>
cd KIT_KlimaCrawler
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
caffeinate -i python3 -m crawler.scripts.run_worker --limit 1
```
