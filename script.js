const pages = {
  "/": {
    title: "Course Overview",
    body: `
      <p class="lede">MSDS 682 introduces the foundations and practice of data streaming: ingesting, processing, analyzing, and operating real-time data systems at scale.</p>
      <p>The course emphasizes clear technical thinking, hands-on implementation, and careful evaluation. Students will work with streaming data concepts, system design tradeoffs, and reproducible workflows for real-time analytics.</p>

      <div class="notice">
        Course materials, readings, assignments, and due dates will be updated throughout the term.
      </div>

      <h3>Logistics</h3>
      <div class="meta-list">
        <div class="meta-row"><strong>Course</strong><span>MSDS 682</span></div>
        <div class="meta-row"><strong>Term</strong><span>Summer 2026</span></div>
        <div class="meta-row"><strong>Format</strong><span>Graduate course with lectures, labs, assignments, and a final project</span></div>
        <div class="meta-row"><strong>Meeting pattern</strong><span>To be announced</span></div>
        <div class="meta-row"><strong>Course materials</strong><span>Readings, notebooks, slides, and assignment links will be posted here</span></div>
      </div>

      <h3>Course Themes</h3>
      <ul>
        <li><strong>Streaming foundations:</strong> events, logs, windows, state, ordering, and delivery guarantees.</li>
        <li><strong>Streaming systems:</strong> ingestion, processing, storage, serving, and operational tradeoffs.</li>
        <li><strong>Real-time analytics:</strong> monitoring, anomaly detection, experimentation, and decision workflows.</li>
        <li><strong>Production thinking:</strong> reliability, observability, reproducibility, and failure handling.</li>
      </ul>

      <h3>Learning Goals</h3>
      <ul>
        <li>Explain core streaming concepts and when streaming architectures are appropriate.</li>
        <li>Build reproducible workflows for ingesting and processing real-time data.</li>
        <li>Evaluate streaming applications using latency, throughput, correctness, and reliability evidence.</li>
        <li>Communicate system design decisions clearly with assumptions and tradeoffs.</li>
      </ul>
    `
  },
  "/schedule": {
    title: "Schedule",
    body: `
      <p class="lede">The schedule below will be updated with readings, labs, and assignment deadlines as the course progresses.</p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Week</th>
              <th>Topic</th>
              <th>Materials</th>
              <th>Due</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>1</td>
              <td>Course framing and streaming data foundations</td>
              <td><span class="tag">TBD</span></td>
              <td>-</td>
            </tr>
            <tr>
              <td>2</td>
              <td>Events, logs, ingestion, and reproducible workflows</td>
              <td><span class="tag">TBD</span></td>
              <td>Assignment 1 released</td>
            </tr>
            <tr>
              <td>3</td>
              <td>Stream processing models, windows, and state</td>
              <td><span class="tag">TBD</span></td>
              <td>-</td>
            </tr>
            <tr>
              <td>4</td>
              <td>Delivery guarantees, ordering, and correctness</td>
              <td><span class="tag">TBD</span></td>
              <td>Assignment 1 due</td>
            </tr>
            <tr>
              <td>5</td>
              <td>Real-time analytics and monitoring</td>
              <td><span class="tag">TBD</span></td>
              <td>Assignment 2 released</td>
            </tr>
            <tr>
              <td>6</td>
              <td>Scaling, partitioning, and backpressure</td>
              <td><span class="tag">TBD</span></td>
              <td>-</td>
            </tr>
            <tr>
              <td>7</td>
              <td>Reliability, observability, and incident analysis</td>
              <td><span class="tag">TBD</span></td>
              <td>Assignment 2 due</td>
            </tr>
            <tr>
              <td>8</td>
              <td>Final project workshops</td>
              <td><span class="tag">TBD</span></td>
              <td>Project proposal due</td>
            </tr>
            <tr>
              <td>9</td>
              <td>Project presentations and review</td>
              <td><span class="tag">TBD</span></td>
              <td>Final project due</td>
            </tr>
          </tbody>
        </table>
      </div>
    `
  },
  "/assignments": {
    title: "Assignments",
    body: `
      <p class="lede">Assignments combine implementation, analysis, and written interpretation. Final instructions, starter code, rubrics, and submission links will be posted here.</p>
      <div class="assignment-list">
        <article class="assignment-card">
          <h3>Assignment 1: Streaming Data Workflow</h3>
          <p>Build a clean, documented workflow for ingesting, transforming, and validating event data.</p>
          <p><span class="tag">Coming soon</span></p>
        </article>
        <article class="assignment-card">
          <h3>Assignment 2: Windowed Processing and Monitoring</h3>
          <p>Implement streaming aggregations and evaluate latency, correctness, and operational failure cases.</p>
          <p><span class="tag">Coming soon</span></p>
        </article>
        <article class="assignment-card">
          <h3>Final Project</h3>
          <p>Develop a practical streaming data application, evaluate it rigorously, and present technical decisions with evidence.</p>
          <p><span class="tag">Coming soon</span></p>
        </article>
      </div>
    `
  },
  "/syllabus": {
    title: "Syllabus",
    body: `
      <p class="lede">This page summarizes course expectations, prerequisites, assessment structure, and policies.</p>

      <h3>Course Description</h3>
      <p>MSDS 682 focuses on data streaming systems and real-time analytics. Students will practice building reliable technical workflows, evaluating outcomes, and communicating design decisions with evidence.</p>

      <h3>Prerequisites</h3>
      <ul>
        <li>Working knowledge of Python.</li>
        <li>Basic probability, statistics, and machine learning familiarity.</li>
        <li>Comfort using notebooks, scripts, Git, and command-line tools.</li>
      </ul>

      <h3>Assessment</h3>
      <div class="meta-list">
        <div class="meta-row"><strong>Assignments</strong><span>TBD</span></div>
        <div class="meta-row"><strong>Final project</strong><span>TBD</span></div>
        <div class="meta-row"><strong>Participation</strong><span>TBD</span></div>
      </div>

      <h3>Policies</h3>
      <p>Attendance, collaboration, late work, academic integrity, and accessibility policies will follow the final course syllabus.</p>
    `
  },
  "/staff": {
    title: "Staff",
    body: `
      <p class="lede">Course staff and support information will be posted here.</p>
      <div class="staff-grid">
        <article class="staff-card">
          <h3>Jeremy Gu</h3>
          <p>Instructor</p>
          <p>Office hours and contact information to be announced.</p>
        </article>
        <article class="staff-card">
          <h3>Teaching Assistant</h3>
          <p>To be announced</p>
          <p>Course support details will be added before launch.</p>
        </article>
      </div>
    `
  }
};

// Handout manifest. To add a handout: drop a file in `handouts/` and add a row
// here. Markdown handouts (kind: "md") render in-page with syntax-highlighted
// code blocks; PDF handouts (kind: "pdf") open the file directly.
const handouts = [
  {
    slug: "example-markdown-handout",
    title: "Example Handout: Reading a Kafka Topic",
    kind: "md",
    file: "handouts/example-markdown-handout.md",
    date: "Template",
    summary: "Template handout showing prose plus a syntax-highlighted code block."
  }
  // PDF example (uncomment and add the file to publish):
  // {
  //   slug: "week1-slides",
  //   title: "Week 1 Slides",
  //   kind: "pdf",
  //   file: "handouts/week1-slides.pdf",
  //   date: "Sep 2026",
  //   summary: "Lecture slides handout (PDF)."
  // }
];

function escapeHtml(value) {
  return value.replace(/[&<>"]/g, (ch) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch]
  ));
}

function handoutsListBody() {
  const cards = handouts.map((h) => {
    const href = h.kind === "pdf" ? h.file : `#/handouts/${h.slug}`;
    const target = h.kind === "pdf" ? ' target="_blank" rel="noopener"' : "";
    const badge = h.kind === "pdf" ? "PDF" : "Markdown";
    return `
      <article class="handout-card">
        <div class="handout-card-head">
          <h3><a href="${href}"${target}>${escapeHtml(h.title)}</a></h3>
          <span class="tag">${badge}</span>
        </div>
        <p>${escapeHtml(h.summary)}</p>
        <p class="handout-meta">${escapeHtml(h.date)}</p>
      </article>`;
  }).join("");

  return `
    <p class="lede">Lecture notes, reference sheets, and slides. Markdown handouts render here with highlighted code; PDF handouts open directly.</p>
    <div class="handout-list">${cards || '<p>No handouts posted yet.</p>'}</div>
  `;
}

const fallbackRoute = "/";
const content = document.querySelector("#content");
const navLinks = [...document.querySelectorAll(".nav a")];

function parseRoute() {
  const hash = window.location.hash.replace(/^#/, "") || fallbackRoute;
  const detail = hash.match(/^\/handouts\/(.+)$/);
  if (detail) {
    return { kind: "handout", slug: detail[1], nav: "/handouts" };
  }
  if (hash === "/handouts") {
    return { kind: "static", route: "/handouts", nav: "/handouts" };
  }
  if (pages[hash]) {
    return { kind: "static", route: hash, nav: hash };
  }
  return { kind: "static", route: fallbackRoute, nav: fallbackRoute };
}

function setActiveNav(navRoute) {
  navLinks.forEach((link) => {
    const active = link.dataset.route === navRoute;
    link.classList.toggle("active", active);
    if (active) {
      link.setAttribute("aria-current", "page");
    } else {
      link.removeAttribute("aria-current");
    }
  });
}

function renderStatic(route) {
  const page = route === "/handouts"
    ? { title: "Handouts", body: handoutsListBody() }
    : pages[route];
  content.innerHTML = `<h2>${page.title}</h2>${page.body}`;
  document.title = `${page.title} - MSDS 682`;
}

async function renderHandout(slug) {
  const meta = handouts.find((h) => h.slug === slug);
  const backLink = '<p class="back-link"><a href="#/handouts">&larr; All handouts</a></p>';

  if (!meta || meta.kind !== "md") {
    content.innerHTML = `${backLink}<h2>Handout not found</h2><p>This handout is not available.</p>`;
    document.title = "Handout not found - MSDS 682";
    return;
  }

  document.title = `${meta.title} - MSDS 682`;
  content.innerHTML = `${backLink}<div class="handout"><p class="lede">Loading…</p></div>`;

  try {
    const res = await fetch(meta.file, { cache: "no-cache" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const md = await res.text();
    const html = window.marked ? window.marked.parse(md) : `<pre>${escapeHtml(md)}</pre>`;
    content.innerHTML = `${backLink}<article class="handout">${html}</article>`;
    if (window.hljs) {
      content.querySelectorAll("pre code").forEach((el) => window.hljs.highlightElement(el));
    }
  } catch (err) {
    content.innerHTML = `${backLink}<h2>${escapeHtml(meta.title)}</h2>` +
      `<div class="notice">Could not load this handout (${escapeHtml(String(err.message))}). ` +
      `Markdown handouts require the site to be served over http (local server or GitHub Pages), not opened as a file.</div>`;
  }
}

async function render() {
  const parsed = parseRoute();
  setActiveNav(parsed.nav);

  if (parsed.kind === "handout") {
    await renderHandout(parsed.slug);
  } else {
    renderStatic(parsed.route);
  }

  content.focus({ preventScroll: true });
}

window.addEventListener("hashchange", render);
render();
