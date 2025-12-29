from __future__ import annotations
from functools import lru_cache
from typing import Optional, Tuple
from lingua import Language, LanguageDetectorBuilder


# Idiomas soportados para el preprocesado.
SUPPORTED = {
    "spanish": Language.SPANISH,
    "english": Language.ENGLISH,
    "french": Language.FRENCH,
    "german": Language.GERMAN,
    "italian": Language.ITALIAN,
    "portuguese": Language.PORTUGUESE,
}

# Umbral de confianza
DEFAULT_MIN_CONFIDENCE = 0.60


@lru_cache(maxsize=1)
def _detector():
    # Sólo cargamos los idiomas soportados
    langs = list(SUPPORTED.values())
    return LanguageDetectorBuilder.from_languages(*langs).build()


def detect_language(
    text: str, min_confidence: float = DEFAULT_MIN_CONFIDENCE
) -> Tuple[str, float]:
    """
    Devuelve (language_code, confidence).
    language_code será uno de los keys de SUPPORTED o "unknown".
    """
    text = (text or "").strip()
    if len(text) < 20:  # texto demasiado corto
        return ("unknown", 0.0)

    det = _detector()
    lang = det.detect_language_of(text)
    if lang is None:
        return ("unknown", 0.0)

    # Lingua da valores de confianza con "detect_language_of()"
    # Utilizo probabilities para una confianza razonable.
    values = det.compute_language_confidence_values(text)
    best = max(values, key=lambda x: x.value, default=None)
    if best is None or best.value < min_confidence:
        return ("unknown", float(best.value) if best else 0.0)

    # Convertir Language -> string soportado
    for code, L in SUPPORTED.items():
        if best.language == L:
            return (code, float(best.value))

    return ("unknown", float(best.value))
