import re
import unicodedata
from typing import List
from functools import lru_cache
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer


TOKEN_RE = re.compile(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+(?:'[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+)?")

# Mapa lingua-code -> nltk language name
NLTK_LANG = {
    "spanish": "spanish",
    "english": "english",
    "french": "french",
    "german": "german",
    "italian": "italian",
    "portuguese": "portuguese",
}


@lru_cache(maxsize=64)
def _stopwords_set(lang: str) -> set[str]:
    nltk_lang = NLTK_LANG.get(lang)
    if not nltk_lang:
        return set()
    try:
        return set(stopwords.words(nltk_lang))
    except LookupError:
        # No se descargaron stopwords en NLTK
        return set()
    except OSError:
        # Idioma no disponible en NLTK
        return set()


@lru_cache(maxsize=64)
def _stemmer(lang: str):
    nltk_lang = NLTK_LANG.get(lang)
    if not nltk_lang:
        return None
    try:
        return SnowballStemmer(nltk_lang)
    except ValueError:
        return None


def lexical_analysis(text: str) -> str:
    # Normaliza unicode + minúsculas + espacios
    text = unicodedata.normalize("NFKC", text)
    text = text.lower()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize(text: str) -> List[str]:
    return TOKEN_RE.findall(text)


def remove_stopwords(tokens: List[str], language: str) -> List[str]:
    sw = _stopwords_set(language)
    if not sw:
        # fallback: sin stopwords si no hay soporte
        return tokens
    return [t for t in tokens if t not in sw]


def filter_meaningful(tokens: List[str], min_len: int = 2) -> List[str]:
    return [t for t in tokens if len(t) >= min_len and not t.isnumeric()]


def lemmatize_or_stem(tokens: List[str], language: str) -> List[str]:
    # Podemos usar stemming o lematización, utilizamos stemming porque es más
    # rápido (conveniente en una corpus de gran tamaño).
    stemmer = _stemmer(language)
    if stemmer is None:
        # fallback: sin stemming si no hay stemmer
        return tokens
    return [stemmer.stem(t) for t in tokens]
