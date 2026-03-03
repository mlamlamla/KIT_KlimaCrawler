import sqlite3
import pandas as pd
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer

def load_data(limit=5000):
    conn = sqlite3.connect('crawler/data/db/crawl.sqlite')
    query = "SELECT text FROM segments WHERE length(text) > 100 LIMIT ?"
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    return df['text'].tolist()

docs = load_data(limit=10000) 

embedding_model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

topic_model = BERTopic(
    embedding_model=embedding_model,
    language="german",
    calculate_probabilities=True,
    verbose=True
)

topics, probs = topic_model.fit_transform(docs)
info = topic_model.get_topic_info()
topic_model.visualize_topics().write_html("topic_map.html")