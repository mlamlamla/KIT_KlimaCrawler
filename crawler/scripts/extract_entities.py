import sqlite3
import json
import time
from pathlib import Path
from typing import List, Optional, Literal
from pydantic import BaseModel, Field
from openai import OpenAI

# ==========================================
# KONFIGURATION
# ==========================================
# Für Pydantic Structured Outputs empfehlen wir OpenAI (gpt-4o-mini ist extrem günstig und schnell).
# Falls du Ollama nutzt, musst du evtl. auf json_mode zurückgreifen, da Structured Outputs dort teils noch experimentell sind.
client = OpenAI() # Greift automatisch auf die Umgebungsvariable OPENAI_API_KEY zu
MODEL_NAME = "gpt-4o-mini" 
DB_PATH = Path("crawler/data/db/crawl.sqlite")

# ==========================================
# DIE ONTOLOGIE FÜR DEIN PAPER (Pydantic Models)
# ==========================================
class Entity(BaseModel):
    name: str = Field(description="Der Name der Entität, z.B. 'Freistaat Bayern', 'E-Ladesäulen Bahnhofstraße', 'PV-Anlage Grundschule'")
    type: Literal["Akteur", "Infrastruktur", "Förderprogramm", "Konzept/Ziel", "Dokument"] = Field(description="Die Art des Knotens im Graphen")
    category: Literal["Mobilität", "Wärme", "Strom", "Finanzen", "Governance", "Sonstiges"] = Field(description="Das übergeordnete Nachhaltigkeitsthema")
    status: Literal["Geplant", "In Umsetzung", "Abgeschlossen", "Existierend", "Unbekannt"] = Field(description="Der aktuelle Status des Projekts oder der Infrastruktur")
    metrics: dict[str, str] = Field(description="Wichtige Metriken als Key-Value-Paare, z.B. {'anzahl': '5', 'kosten_euro': '20000', 'leistung_kwp': '100'}. Leer lassen, wenn keine Metriken im Text stehen.", default_factory=dict)

class Relationship(BaseModel):
    source_entity: str = Field(description="Der Name der ausgehenden Entität (muss in der Entity-Liste existieren)")
    relation_type: Literal["FÖRDERT", "BAUT", "BESCHLIESST", "GEHÖRT_ZU", "KOOPERIERT_MIT", "BEZIEHT_SICH_AUF"] = Field(description="Die Art der Beziehung (die Kante im Graphen)")
    target_entity: str = Field(description="Der Name der Zielentität (muss in der Entity-Liste existieren)")
    evidence: str = Field(description="Ein kurzer Belegsatz aus dem Text, der diese Beziehung beweist (wichtig für die Nachvollziehbarkeit im Paper)")

class KnowledgeGraph(BaseModel):
    entities: List[Entity] = Field(description="Liste aller extrahierten Knoten")
    relationships: List[Relationship] = Field(description="Liste aller Beziehungen zwischen den Knoten")

# ==========================================
# DATENBANK & EXTRAKTION
# ==========================================
def setup_database(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS graph_triplets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            municipality_id TEXT,
            document_id TEXT,
            segment_rowid INTEGER,
            graph_json TEXT,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

def get_unprocessed_segments(cursor, limit=5):
    # Wir filtern grob nach Signalwörtern, um Token-Kosten zu sparen
    query = """
        SELECT s.rowid, d.municipality_id, d.document_id, s.text 
        FROM segments s
        JOIN documents_raw d ON s.document_id = d.document_id
        WHERE (s.text LIKE '%Klima%' OR s.text LIKE '%Energie%' OR s.text LIKE '%Rad%' OR s.text LIKE '%Solar%' OR s.text LIKE '%Wärme%' OR s.text LIKE '%Ladesäule%')
          AND s.rowid NOT IN (SELECT segment_rowid FROM graph_triplets)
        LIMIT ?;
    """
    cursor.execute(query, (limit,))
    return cursor.fetchall()

def extract_graph_data(text: str) -> Optional[str]:
    prompt = f"""
    Du bist ein wissenschaftlicher Assistent. Extrahiere aus dem folgenden Text alle Entitäten und Beziehungen zum Thema kommunale Klimapolitik, Nachhaltigkeit und Infrastruktur.
    Ignoriere irrelevante Informationen. Wenn der Text keine verwertbaren Fakten enthält, gib leere Listen zurück.
    
    Text:
    {text}
    """
    try:
        # Die Magie: Wir nutzen client.beta.chat.completions.parse, um das Pydantic-Schema zu erzwingen!
        response = client.beta.chat.completions.parse(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "Du extrahierst strikt strukturierte Daten für einen Knowledge Graph."},
                {"role": "user", "content": prompt}
            ],
            response_format=KnowledgeGraph,
            temperature=0.0
        )
        # Pydantic-Objekt als sauberes JSON zurückgeben
        return response.choices[0].message.content
    except Exception as e:
        print(f"⚠️ LLM Fehler: {e}")
        return None

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    setup_database(cursor)
    conn.commit()

    segments = get_unprocessed_segments(cursor, limit=5)
    if not segments:
        print("✅ Keine neuen Segmente für den Graphen gefunden.")
        return

    print(f"🚀 Starte Graph-Extraktion für {len(segments)} Segmente...\n")

    for rowid, muni_id, doc_id, text in segments:
        print(f"⚙️ Verarbeite Gemeinde: {muni_id} | Segment: {rowid}")
        
        json_result = extract_graph_data(text)
        
        if json_result:
            try:
                parsed = json.loads(json_result)
                e_count = len(parsed.get('entities', []))
                r_count = len(parsed.get('relationships', []))
                print(f"   🟢 Gefunden: {e_count} Knoten (Entities) | {r_count} Kanten (Relationships)")
                
                # Speichern in die neue, saubere Tabelle
                cursor.execute("""
                    INSERT INTO graph_triplets (municipality_id, document_id, segment_rowid, graph_json)
                    VALUES (?, ?, ?, ?)
                """, (muni_id, doc_id, rowid, json_result))
                conn.commit()
            except json.JSONDecodeError:
                print("   🔴 Fehler beim Parsen des LLM-Ergebnisses.")
        
        time.sleep(1) # API Rate Limit schonen

    conn.close()
    print("\n🏁 Lauf abgeschlossen.")

if __name__ == "__main__":
    main()