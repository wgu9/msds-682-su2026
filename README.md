# MSDS 682 · Data Streaming — Summer 2026

Course website for **MSDS 682 (Data Streaming)** at the University of San Francisco.

A lightweight static site — no framework, no build step — inspired by minimal
academic course pages. Sections:

- **Home** — overview, logistics, learning goals
- **Schedule** — week-by-week plan
- **Assignments** — assignment briefs
- **Handouts** — Markdown notes (with highlighted code) and PDF handouts
- **Syllabus** — policies and expectations
- **Staff** — instructor and support

## Structure

```
index.html   Shell: header, nav, content mount
script.js    Page content + hash router + handout renderer
styles.css   Visual system (USF green/gold, minimal)
assets/      usf-logo.svg (swap with the official logo)
handouts/    .md and .pdf handouts
```

## Adding a handout

1. Drop a file in `handouts/` — a Markdown file (`.md`) or a PDF.
2. Add one row to the `handouts` array in `script.js`:

   ```js
   {
     slug: "week2-consumers",       // becomes #/handouts/week2-consumers
     title: "Week 2: Consumers",
     kind: "md",                    // "md" renders in-page, "pdf" opens the file
     file: "handouts/week2-consumers.md",
     date: "Sep 2026",
     summary: "One-line description."
   }
   ```

Markdown handouts render in the page with syntax-highlighted code blocks
(via marked + highlight.js, loaded from CDN). PDF handouts open directly.

## Local preview

```bash
python3 -m http.server 8000
```

Open `http://localhost:8000`. Serving over HTTP is required — Markdown handouts
are fetched at runtime and will not load from a `file://` path.

## Publishing

To publish on GitHub Pages: repository **Settings → Pages → Deploy from branch →
`main` / root**. The `.nojekyll` file is already present so asset paths are served
as-is.
