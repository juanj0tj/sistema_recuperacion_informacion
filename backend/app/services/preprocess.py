import re
import unicodedata
from typing import List
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer


TOKEN_RE = re.compile(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+(?:'[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+)?")


def lexical_analysis(text: str) -> str:
    # Normaliza unicode + minúsculas + espacios
    text = unicodedata.normalize("NFKC", text)
    text = text.lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize(text: str) -> List[str]:
    return TOKEN_RE.findall(text)


def remove_stopwords(tokens: List[str], language: str = "spanish") -> List[str]:
    sw = set(stopwords.words(language))
    return [t for t in tokens if t not in sw]


def filter_meaningful(tokens: List[str], min_len: int = 2) -> List[str]:
    return [t for t in tokens if len(t) >= min_len and not t.isnumeric()]


def lematize_or_stem(tokens: List[str], language: str = "spanish") -> List[str]:
    # Podemos usar stemming o lematización, utilizamos stemming porque es más
    # rápido (conveniente en una corpus de gran tamaño).
    stemmer = SnowballStemmer(language)
    return [stemmer.stem(t) for t in tokens]
