import sqlite3
import re
import textwrap
from pathlib import Path

COLOR_MONEY = '\033[92m' 
COLOR_ACTOR = '\033[96m'
COLOR_RESET = '\033[0m'

def highlight_text(text: str) -> str:
    money_pattern = r'(\d{1,3}(?:\.\d{3})*(?:,\d+)?\s*(?:€|Euro|Mio|Tsd|Mrd))'
    text = re.sub(money_pattern, rf"{COLOR_MONEY}\1{COLOR_RESET}", text, flags=re.IGNORECASE)
    
    trigger_words = [
        'Förder', 'Zuschuss', 'Invest', 'Finanz', 'Spende', 'Stiftung', 
        'Kooperation', 'unterstützt', 'gefördert', 'Beteiligung', 'Genossenschaft',
        'GmbH', 'AG', 'Verein'
    ]
    for trigger in trigger_words:
        text = re.sub(rf'({trigger}[a-zA-Z]*)', rf"{COLOR_ACTOR}\1{COLOR_RESET}", text, flags=re.IGNORECASE)
        
    return text

def analyze_finances():
    db_path = Path("crawler/data/db/crawl.sqlite")
    
    if not db_path.exists():
        print(f"❌ Datenbank nicht gefunden unter: {db_path}")
        print("Bitte zuerst den Crawler starten und einige Daten sammeln!")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
        SELECT d.municipality_id, d.url_canonical, s.text 
        FROM segments s
        JOIN documents_raw d ON s.document_id = d.document_id
        WHERE (s.text LIKE '%€%' OR s.text LIKE '%Euro%' OR s.text LIKE '%Mio.%')
          AND (s.text LIKE '%KfW%' OR s.text LIKE '%EU-%' OR s.text LIKE '%Bund%' 
               OR s.text LIKE '%Förder%' OR s.text LIKE '%Investition%' OR s.text LIKE '%Zuschuss%')
          AND (s.text LIKE '%Klima%' OR s.text LIKE '%Energie%' OR s.text LIKE '%Rad%' OR s.text LIKE '%Solar%')
        LIMIT 15;
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
    except sqlite3.OperationalError as e:
        print(f"❌ SQL-Fehler (vielleicht heißt die Spalte in 'segments' anders?): {e}")
        return
    finally:
        conn.close()

    print(f"\n{'='*80}")
    print(f"🚀 FINANZ-DETEKTOR: {len(results)} GraphRAG-Kandidaten gefunden!")
    print(f"{'='*80}\n")

    if not results:
        print("Bisher noch keine passenden Segmente gecrawlt. Lass den Crawler noch etwas laufen!")
        return

    for muni_id, url, text in results:
        print(f"📍 Gemeinde:  {muni_id}")
        print(f"🔗 Quelle:    {url}")
        print("-" * 80)
        
        clean_text = " ".join(text.split())
        highlighted = highlight_text(clean_text)
        
        wrapped_text = textwrap.fill(highlighted, width=100)
        print(wrapped_text)
        print(f"\n{'='*80}\n")

if __name__ == "__main__":
    analyze_finances()