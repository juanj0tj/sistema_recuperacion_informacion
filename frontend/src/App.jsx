import { useMemo, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE || "";
const PAGE_SIZE = 10;

const LANGUAGES = [
  { id: "spanish", label: "Español", short: "es" },
  { id: "english", label: "English", short: "en" },
  { id: "french", label: "Français", short: "fr" },
  { id: "german", label: "Deutsch", short: "de" },
  { id: "italian", label: "Italiano", short: "it" },
  { id: "portuguese", label: "Português", short: "pt" },
];

const buildEndpoint = (query, language) => {
  const params = new URLSearchParams({
    query,
    default_language: language,
  });
  if (API_BASE) {
    return `${API_BASE}/search?${params.toString()}`;
  }
  return `/search?${params.toString()}`;
};

export default function App() {
  const [query, setQuery] = useState("");
  const [language, setLanguage] = useState(LANGUAGES[0].id);
  const [results, setResults] = useState([]);
  const [status, setStatus] = useState("idle");
  const [error, setError] = useState("");
  const [page, setPage] = useState(1);
  const [lastQuery, setLastQuery] = useState("");

  const canSearch = query.trim().length > 0;
  const totalPages = Math.ceil(results.length / PAGE_SIZE);
  const shouldPaginate = results.length > PAGE_SIZE;

  const visibleResults = useMemo(() => {
    if (!shouldPaginate) return results;
    const start = (page - 1) * PAGE_SIZE;
    return results.slice(start, start + PAGE_SIZE);
  }, [page, results, shouldPaginate]);

  const handleSearch = async (event) => {
    event.preventDefault();
    const trimmed = query.trim();

    if (!trimmed) {
      setError("Introduce una query para buscar.");
      setStatus("error");
      return;
    }

    setStatus("loading");
    setError("");
    setResults([]);
    setPage(1);
    setLastQuery(trimmed);

    try {
      const response = await fetch(buildEndpoint(trimmed, language));
      if (!response.ok) {
        let message = `Error ${response.status}`;
        try {
          const payload = await response.json();
          if (payload && payload.detail) {
            message = payload.detail;
          }
        } catch {
          // ignore JSON parsing errors
        }
        throw new Error(message);
      }

      const data = await response.json();
      setResults(Array.isArray(data.results) ? data.results : []);
      setStatus("success");
    } catch (err) {
      setStatus("error");
      setError(err instanceof Error ? err.message : "Error inesperado");
    }
  };

  const handlePageChange = (nextPage) => {
    if (nextPage < 1 || nextPage > totalPages) return;
    setPage(nextPage);
  };

  return (
    <div className="app">
      <header className="header">
        <div className="brand">
          <span className="brand-mark" />
          Sistema de Recuperación de Información
        </div>
      </header>

      <section className="hero">
        <div className="language-panel">
          <div className="language-title">Idioma por defecto de la query</div>
          <div className="language-grid">
            {LANGUAGES.map((lang) => (
              <button
                key={lang.id}
                type="button"
                className={`language-button ${
                  language === lang.id ? "is-active" : ""
                }`}
                onClick={() => setLanguage(lang.id)}
                aria-pressed={language === lang.id}
              >
                <span className={`flag flag-${lang.short}`} aria-hidden="true" />
                <span>{lang.label}</span>
              </button>
            ))}
          </div>
          <div className="hint">
            Se aplica si la deteccion automatica no reconoce el idioma.
          </div>
        </div>
      </section>

      <form className="search-card" onSubmit={handleSearch}>
        <input
          className="search-input"
          type="text"
          placeholder="Ejemplo: neural information retrieval"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
        <button className="search-button" type="submit" disabled={!canSearch || status === "loading"}>
          {status === "loading" ? "Buscando..." : "Buscar"}
        </button>
      </form>

      {status === "loading" && (
        <div className="status">
          <span className="spinner" />
          Consultando el indice...
        </div>
      )}

      {status === "error" && (
        <div className="status error">{error || "No se pudo buscar"}</div>
      )}

      {status === "success" && (
        <section className="results">
          <div className="results-header">
            <div className="results-count">
              {results.length} resultados para "{lastQuery}"
            </div>
            {shouldPaginate && (
              <div className="header-note">
                Pagina {page} de {totalPages}
              </div>
            )}
          </div>

          {results.length === 0 ? (
            <div className="status">Sin resultados para esta query.</div>
          ) : (
            <ul className="results-list">
              {visibleResults.map((item, index) => {
                const scoreValue = Number(item.score);
                const scoreLabel = Number.isFinite(scoreValue)
                  ? scoreValue.toFixed(4)
                  : "n/a";

                return (
                  <li
                    key={`${item.doc_id}-${index}`}
                    className="result-card"
                    style={{ "--delay": `${index * 45}ms` }}
                  >
                    <div className="result-meta">
                      #{(page - 1) * PAGE_SIZE + index + 1} - score {scoreLabel}
                    </div>
                    {item.url ? (
                      <a
                        className="result-title"
                        href={item.url}
                        target="_blank"
                        rel="noreferrer"
                      >
                        {item.title || "Documento sin titulo"}
                      </a>
                    ) : (
                      <span className="result-title">
                        {item.title || "Documento sin titulo"}
                      </span>
                    )}
                    <p className="result-snippet">
                      {item.snippet || "Snippet no disponible para este documento."}
                    </p>
                  </li>
                );
              })}
            </ul>
          )}

          {shouldPaginate && (
            <div className="pagination">
              <button
                type="button"
                className="page-button"
                onClick={() => handlePageChange(page - 1)}
                disabled={page === 1}
              >
                Anterior
              </button>
              {Array.from({ length: totalPages }, (_, idx) => idx + 1).map(
                (pageNumber) => (
                  <button
                    key={pageNumber}
                    type="button"
                    className={`page-button ${
                      pageNumber === page ? "is-active" : ""
                    }`}
                    onClick={() => handlePageChange(pageNumber)}
                  >
                    {pageNumber}
                  </button>
                )
              )}
              <button
                type="button"
                className="page-button"
                onClick={() => handlePageChange(page + 1)}
                disabled={page === totalPages}
              >
                Siguiente
              </button>
            </div>
          )}
        </section>
      )}
    </div>
  );
}
