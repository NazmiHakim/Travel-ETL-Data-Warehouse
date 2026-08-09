import re
import logging
from typing import List, Dict, Any
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# ---------------------------------------------------------------------------
# Optional: sentence-transformers for dense semantic embeddings.
# Falls back gracefully to TF-IDF if the library is not installed.
# Install: pip install sentence-transformers
# ---------------------------------------------------------------------------
try:
    from sentence_transformers import SentenceTransformer
    _SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    _SENTENCE_TRANSFORMERS_AVAILABLE = False
    logging.getLogger(__name__).debug(
        "sentence-transformers not installed — using TF-IDF retrieval (run: pip install sentence-transformers)"
    )


# ---------------------------------------------------------------------------
# Data Dictionary Knowledge Base
# ---------------------------------------------------------------------------
DATA_DICTIONARY: List[Dict[str, Any]] = [
    {
        "id": "airline_carriers",
        "category": "Dim_Airline",
        "title": "Airline Carrier Codes & Brands",
        "content": (
            "dim_airline maps carrier codes to full airline names: "
            "AA = American Airlines, UA = United Airlines, DL = Delta Air Lines, "
            "WN = Southwest Airlines, F9 = Frontier Airlines, NK = Spirit Airlines, "
            "B6 = JetBlue Airways, AS = Alaska Airlines. "
            "Keywords: richest, poorest, carrier, airline, brand."
        ),
        "aliases": {
            "aa": "American Airlines",
            "american": "American Airlines",
            "ua": "United Airlines",
            "united": "United Airlines",
            "dl": "Delta Air Lines",
            "delta": "Delta Air Lines",
            "wn": "Southwest Airlines",
            "southwest": "Southwest Airlines",
            "f9": "Frontier Airlines",
            "frontier": "Frontier Airlines",
            "nk": "Spirit Airlines",
            "spirit": "Spirit Airlines",
            "b6": "JetBlue Airways",
            "jetblue": "JetBlue Airways",
            "as": "Alaska Airlines",
            "alaska": "Alaska Airlines",
        },
    },
    {
        "id": "airport_iata_codes",
        "category": "Dim_Airport",
        "title": "Airport IATA Codes & Cities",
        "content": (
            "dim_airport stores airport locations and IATA codes: "
            "ATL = Atlanta (Hartsfield-Jackson), DTW = Detroit, BNA = Nashville, "
            "SLC = Salt Lake City, JFK = New York (JFK Airport), PDX = Portland, "
            "STL = St. Louis, CVG = Cincinnati, LAX = Los Angeles, IAH = Houston, "
            "ORD = Chicago (O'Hare), DFW = Dallas/Fort Worth, SFO = San Francisco, "
            "MCO = Orlando, PHX = Phoenix."
        ),
        "aliases": {
            "atl": "Atlanta",
            "atlanta": "Atlanta",
            "dtw": "Detroit",
            "detroit": "Detroit",
            "bna": "Nashville",
            "nashville": "Nashville",
            "slc": "Salt Lake City",
            "jfk": "New York",
            "pdx": "Portland",
            "stl": "St. Louis",
            "cvg": "Cincinnati",
            "lax": "Los Angeles",
            "iah": "Houston",
            "houston": "Houston",
            "ord": "Chicago",
            "chicago": "Chicago",
            "dfw": "Dallas",
            "dallas": "Dallas",
            "sfo": "San Francisco",
            "mco": "Orlando",
            "orlando": "Orlando",
            "phx": "Phoenix",
            "phoenix": "Phoenix",
        },
    },
    {
        "id": "financial_metrics",
        "category": "Fact_Flights",
        "title": "Revenue & Financial Metrics",
        "content": (
            "total_revenue: SUM(ff.total_revenue) represents gross ticket earnings in USD. "
            "Richest/highest revenue airlines are sorted by SUM(total_revenue) DESC. "
            "Poorest/lowest revenue airlines are sorted by SUM(total_revenue) ASC. "
            "PRASM / Average Fare = SUM(total_revenue) / SUM(total_passengers)."
        ),
        "aliases": {},
    },
    {
        "id": "flight_delays",
        "category": "Fact_Flights",
        "title": "Operational Flight Delays & Punctuality",
        "content": (
            "departure_delay: ff.departure_delay in minutes. "
            "arrival_delay: ff.arrival_delay in minutes. "
            "On-Time Performance (OTP) is departure_delay <= 15 minutes. "
            "Worst delay = AVG(departure_delay) DESC."
        ),
        "aliases": {},
    },
    {
        "id": "customer_feedback",
        "category": "Fact_Customer_Feedback",
        "title": "Customer Reviews, Sentiment & Complaints",
        "content": (
            "fact_customer_feedback contains customer sentiment and complaints. "
            "sentiment values: 'Negative', 'Positive', 'Neutral'. "
            "complaint_category values: 'Service', 'Baggage', 'Delay', 'None'. "
            "satisfaction_score: Numeric score from 1.0 (lowest) to 5.0 (highest). "
            "Worst reviews = sentiment = 'Negative' OR satisfaction_score <= 2."
        ),
        "aliases": {},
    },
    {
        "id": "time_dimensions",
        "category": "Dim_Date",
        "title": "Temporal Trends & Date Dimensions",
        "content": (
            "dim_date provides temporal granularity: dd.year (2024, 2025), "
            "dd.month (1-12), dd.quarter (Q1-Q4), dd.day_of_week. "
            "Monthly trend groups by dd.year, dd.month. "
            "Quarterly trend groups by dd.year, dd.quarter."
        ),
        "aliases": {},
    },
]


class DataDictionaryVectorRAG:
    """
    Hybrid Vector RAG Retriever for the TravelNusantara Data Dictionary.

    Retrieval Strategy (automatic selection):
    - **Dense Embeddings (preferred):** Uses `sentence-transformers/all-MiniLM-L6-v2`
      when the `sentence-transformers` package is installed. This model handles
      paraphrased queries (e.g., 'earnings' vs 'revenue', 'lateness' vs 'delay')
      that keyword-based methods miss.
    - **Sparse TF-IDF (fallback):** Used automatically if `sentence-transformers`
      is not installed. Fast, zero-dependency, and sufficient for direct keyword matches.

    Both strategies expose identical retrieve() / get_domain_scores() interfaces.
    """

    def __init__(self, dictionary_docs: List[Dict[str, Any]] = DATA_DICTIONARY):
        self.docs = dictionary_docs
        self.corpus = [doc["content"] for doc in self.docs]

        # --- Dense embedding path (sentence-transformers) ---
        self._use_dense = False
        if _SENTENCE_TRANSFORMERS_AVAILABLE:
            try:
                self._encoder = SentenceTransformer("all-MiniLM-L6-v2")
                # Encode all corpus documents once at init time
                self._doc_embeddings = self._encoder.encode(
                    self.corpus, convert_to_numpy=True, show_progress_bar=False
                )
                self._use_dense = True
                logging.getLogger(__name__).debug(
                    "DataDictionaryVectorRAG: using dense embeddings (all-MiniLM-L6-v2)"
                )
            except Exception as e:
                logging.getLogger(__name__).warning(
                    f"sentence-transformers init failed ({e}). Falling back to TF-IDF."
                )

        # --- Sparse TF-IDF path (fallback) ---
        if not self._use_dense:
            self.vectorizer = TfidfVectorizer(stop_words="english")
            self.doc_vectors = self.vectorizer.fit_transform(self.corpus)

        # Build alias map for entity resolution (e.g. JFK -> New York, AA -> American Airlines)
        self.alias_map = {}
        for doc in self.docs:
            for alias, target in doc.get("aliases", {}).items():
                self.alias_map[alias.lower()] = target

    def _compute_similarities(self, query: str) -> np.ndarray:
        """
        Computes cosine similarity scores between the query and all corpus documents.
        Uses dense embeddings if available, otherwise falls back to TF-IDF.

        Args:
            query: The user's natural language query.

        Returns:
            A 1D numpy array of similarity scores, one per corpus document.
        """
        if self._use_dense:
            query_embedding = self._encoder.encode([query], convert_to_numpy=True, show_progress_bar=False)
            # cosine_similarity returns shape (1, n_docs) — flatten to 1D
            return cosine_similarity(query_embedding, self._doc_embeddings).flatten()
        else:
            query_vec = self.vectorizer.transform([query])
            return cosine_similarity(query_vec, self.doc_vectors).flatten()

    def retrieve(self, query: str, top_k: int = 2) -> List[Dict[str, Any]]:
        """
        Retrieves the top_k most semantically relevant data dictionary chunks
        for a given user query.

        Uses dense embeddings (sentence-transformers) when available for
        paraphrase-robust retrieval; falls back to TF-IDF otherwise.

        Args:
            query: The user's natural language query.
            top_k: Maximum number of chunks to return.

        Returns:
            List of matching data dictionary dicts, each augmented with
            a 'similarity_score' key. Returns [] for empty queries.
        """
        if not query or not query.strip():
            return []

        similarities = self._compute_similarities(query)
        ranked_indices = similarities.argsort()[::-1]

        # Relevance threshold: 0.05 for TF-IDF, 0.15 for dense (different score scales)
        threshold = 0.15 if self._use_dense else 0.05

        results = []
        for idx in ranked_indices[:top_k]:
            score = float(similarities[idx])
            if score > threshold:
                doc = self.docs[idx].copy()
                doc["similarity_score"] = round(score, 4)
                results.append(doc)
        return results

    def get_domain_scores(self, query: str) -> Dict[str, float]:
        """
        Returns a dictionary mapping each document ID (e.g., 'customer_feedback')
        to its cosine similarity score for the given query.

        Used by generate_dynamic_sql() to select the correct SQL template
        based on domain relevance (e.g., feedback_score >= 0.12).

        Args:
            query: The user's natural language query.

        Returns:
            Dict of {doc_id: similarity_score}. Empty dict for empty query.
        """
        if not query or not query.strip():
            return {}
        similarities = self._compute_similarities(query)
        return {self.docs[i]["id"]: round(float(similarities[i]), 4) for i in range(len(self.docs))}


    def normalize_entities(self, text: str) -> Dict[str, Any]:
        """
        Scans text for IATA codes, carrier acronyms, or city aliases
        and returns normalized entity replacements.
        """
        tokens = re.findall(r"\b[A-Za-z0-9]+\b", text)
        replacements = {}
        for token in tokens:
            t_lower = token.lower()
            if t_lower in self.alias_map:
                replacements[token] = self.alias_map[t_lower]
        return replacements

    def format_rag_context(self, query: str) -> str:
        """
        Formats retrieved RAG knowledge into a markdown string suitable
        for LLM context grounding.
        """
        retrieved_docs = self.retrieve(query, top_k=2)
        if not retrieved_docs:
            return "No specific data dictionary rules triggered."

        lines = ["[Retrieved Data Dictionary RAG Context]"]
        for doc in retrieved_docs:
            lines.append(
                f"- **{doc['title']}** (Relevance: {doc['similarity_score']}): {doc['content']}"
            )

        entities = self.normalize_entities(query)
        if entities:
            lines.append("- **Entity Normalizations:** " + ", ".join(f"{k} -> {v}" for k, v in entities.items()))

        return "\n".join(lines)
