# Frontend - Sistema de Recuperacion de Informacion

Este frontend es una aplicacion React construida con Vite. Permite realizar busquedas, seleccionar el idioma por defecto de la query y navegar resultados con paginacion cuando el backend devuelve mas de 10 documentos.

El objetivo es ofrecer una interfaz moderna, sobria y clara para consultar el indice TF-IDF expuesto por el backend.

---

## Stack

- React 18 (UI y estado)
- Vite (bundler y servidor de desarrollo)
- CSS vanilla (estilos y animaciones)

---

## Estructura del proyecto

```
frontend/
├── .env                  # Variables de entorno
├── index.html            # HTML base con el root de React
├── package.json          # Dependencias y scripts
├── vite.config.js        # Configuracion de Vite + proxy
├── README.md             # Esta documentacion
└── src/
    ├── App.jsx             # Componente principal
    ├── main.jsx            # Punto de entrada React
    └── styles.css          # Estilos globales
```

---

## Flujo y funcionamiento

1) El usuario escribe una query y selecciona el idioma por defecto.
2) Al enviar el formulario, el frontend llama a `GET /search` con los parametros:
   - `query`: el texto de busqueda.
   - `default_language`: idioma seleccionado (solo se usa si la deteccion automatica falla).
3) El backend devuelve un array `results`.
4) El frontend muestra titulo, snippet y score. El titulo es un enlace al `url` del documento.
5) Si hay mas de 10 resultados, se activa la paginacion local (10 por pagina).

Estados principales en `App.jsx`:
- `query`: texto actual.
- `language`: idioma seleccionado.
- `results`: resultados del backend.
- `status`: `idle | loading | success | error`.
- `page`: pagina activa.

---

## Paginacion

- El frontend pagina en el cliente con un `PAGE_SIZE` fijo de 10.
- Si `results.length > 10`, se muestran controles Anterior / Siguiente y botones de pagina.
- El contador de resultados muestra el total y la pagina actual.

---

## Idiomas y banderas

Idiomas soportados en el selector (coinciden con el backend):
- Español
- Inglés
- Francés
- Alemán
- Italiano
- Portugués

---

## API, respuesta esperada

El frontend espera una respuesta como:

```json
{
  "query": "texto",
  "results": [
    {
      "doc_id": "...",
      "score": 0.1234,
      "title": "Titulo",
      "snippet": "Resumen...",
      "url": "https://..."
    }
  ]
}
```

Si `url` no existe, el titulo se muestra como texto sin enlace.

---

## Configuracion

Variables de entorno (Vite):
- `VITE_API_BASE`: base del backend (ej. `http://localhost:8000`).

Si no se define, el frontend usa el proxy de Vite y llama a `/search` directamente.

---

## Puesta en marcha del servidor frontend

```bash
npm install
npm run dev
```

- Dev server en `http://localhost:5173`.
- Proxy definido en `vite.config.js` para `/search` hacia `http://localhost:8000`.

---

## Estilos y tema

Los estilos son definidos en `src/styles.css` y se aplican globalmente.

---

## Troubleshooting

- Sin resultados: asegurate de tener el indice creado en el backend.
- Errores 400/422: revisa que `default_language` sea uno de los idiomas soportados.
