// Generates the two DAGonFS slide decks of docs/presentations/.
//
//   npm install pptxgenjs sharp
//   node docs/presentations/make_decks.js [path/to/apply_theme.js]
//
// Every number shown on the slides comes from docs/PERFORMANCE.md and
// docs/TESTING.md (Slurm jobs 19513-19516, November 2026). Regenerate the
// decks when those documents change.

const path = require("path");
const fs = require("fs");
const pptxgen = require("pptxgenjs");
const sharp = require("sharp");

const ROOT = path.resolve(__dirname, "..", "..");
const OUT = __dirname;
const FIG = (f) => path.join(ROOT, "figures", f);
const applyThemePath = process.argv[2];
const applyTheme = applyThemePath ? require(path.resolve(applyThemePath)).applyTheme : null;

// ---------------------------------------------------------------- theme
const THEME = {
  name: "DAGonFS",
  headFontFace: "Cambria",
  bodyFontFace: "Calibri",
  colors: {
    dk1: "16213A", // ink: dark slides, titles
    lt1: "FFFFFF",
    dk2: "4A5578", // secondary text
    lt2: "EEF2F7", // card tint
    accent1: "0E9AA7", // teal: DAGonFS client-server
    accent2: "F2A541", // amber: highlights
    accent3: "C8553D", // rust: NFS
    accent4: "2D4E8A", // indigo: DAGonFS peer-to-peer
    accent5: "A0A8B5", // grey: tmpfs bound
    accent6: "6FB98F",
    hlink: "0E9AA7",
    folHlink: "2D4E8A",
  },
};
const H = THEME.colors; // hex, for hex-only options (charts, shadows)
const SERIES = { nfs: H.accent3, cs: H.accent1, p2p: H.accent4, tmpfs: H.accent5 };
const MONO = "Courier New";
const W = 13.333;

function newDeck(title, footer) {
  const pres = new pptxgen();
  pres.layout = "LAYOUT_WIDE";
  pres.title = title;
  pres.subject = "DAGonFS";
  pres.author = "DAGonFS project";
  pres.theme = { headFontFace: THEME.headFontFace, bodyFontFace: THEME.bodyFontFace };
  const C = pres.SchemeColor;

  pres.defineSlideMaster({
    title: "Title (dark)",
    background: { color: C.text1 },
    objects: [
      { placeholder: { options: { name: "title", type: "title", align: "left", x: 0.8, y: 1.9, w: 11.7, h: 1.9, fontSize: 40, bold: true, color: C.background1, valign: "bottom", fontFace: "+mj-lt" }, text: "" } },
      { placeholder: { options: { name: "subtitle", type: "body", x: 0.8, y: 3.95, w: 11.7, h: 1.0, fontSize: 20, color: C.accent2, valign: "top" }, text: "" } },
      { placeholder: { options: { name: "meta", type: "body", x: 0.8, y: 5.7, w: 11.7, h: 1.0, fontSize: 14, color: C.background2, valign: "top" }, text: "" } },
    ],
  });
  pres.defineSlideMaster({
    title: "Content",
    background: { color: C.background1 },
    margin: [0.5, 0.6, 0.6, 0.6],
    objects: [
      { placeholder: { options: { name: "title", type: "title", align: "left", x: 0.6, y: 0.35, w: 12.1, h: 0.85, fontSize: 32, bold: true, color: C.text1, valign: "middle", fontFace: "+mj-lt" }, text: "" } },
      { text: { text: footer, options: { x: 0.6, y: 7.0, w: 9, h: 0.3, fontSize: 10, color: C.text2, margin: 0 } } },
    ],
    slideNumber: { x: 12.1, y: 7.0, w: 0.6, h: 0.3, fontSize: 10, color: C.text2, align: "right", margin: 0 },
  });
  // Same frame with a narrower title, leaving room for the tutorial's step tracker.
  pres.defineSlideMaster({
    title: "Content (step)",
    background: { color: C.background1 },
    margin: [0.5, 0.6, 0.6, 0.6],
    objects: [
      { placeholder: { options: { name: "title", type: "title", align: "left", x: 0.6, y: 0.35, w: 7.5, h: 0.85, fontSize: 28, bold: true, color: C.text1, valign: "middle", fontFace: "+mj-lt" }, text: "" } },
      { text: { text: footer, options: { x: 0.6, y: 7.0, w: 9, h: 0.3, fontSize: 10, color: C.text2, margin: 0 } } },
    ],
    slideNumber: { x: 12.1, y: 7.0, w: 0.6, h: 0.3, fontSize: 10, color: C.text2, align: "right", margin: 0 },
  });
  pres.defineSlideMaster({
    title: "Closing (dark)",
    background: { color: C.text1 },
    objects: [
      { placeholder: { options: { name: "title", type: "title", align: "left", x: 0.8, y: 0.6, w: 11.7, h: 1.1, fontSize: 40, bold: true, color: C.background1, valign: "middle", fontFace: "+mj-lt" }, text: "" } },
    ],
    slideNumber: { x: 12.1, y: 7.0, w: 0.6, h: 0.3, fontSize: 10, color: C.background2, align: "right", margin: 0 },
  });
  return { pres, C };
}

// ---------------------------------------------------------------- helpers
function helpers(pres, C) {
  const t = {};
  // Numbered circle: the deck's motif.
  t.badge = (s, x, y, label, fill, d = 0.5, fontSize = 16) => {
    s.addShape(pres.shapes.OVAL, { x, y, w: d, h: d, fill: { color: fill || C.accent1 }, line: { color: fill || C.accent1 }, objectName: `badge ${label}` });
    s.addText(String(label), { x, y, w: d, h: d, fontSize, bold: true, color: C.background1, align: "center", valign: "middle", margin: 0, isTextBox: true });
  };
  t.card = (s, x, y, w, h, fill) =>
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x, y, w, h, rectRadius: 0.12, fill: { color: fill || C.background2 }, line: { color: fill || C.background2 }, objectName: "card" });
  // Card with a numbered badge, a bold header and a body.
  t.point = (s, x, y, w, h, n, head, body, opts = {}) => {
    t.card(s, x, y, w, h, opts.fill);
    t.badge(s, x + 0.25, y + 0.25, n, opts.badge);
    s.addText(head, { x: x + 0.95, y: y + 0.2, w: w - 1.15, h: 0.6, fontSize: opts.headSize || 18, bold: true, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
    s.addText(body, { x: x + 0.95, y: y + 0.85, w: w - 1.15, h: h - 1.0, fontSize: opts.bodySize || 15, color: C.text2, valign: "top", margin: 0, isTextBox: true, paraSpaceAfter: 4 });
  };
  t.stat = (s, x, y, w, value, label, color) => {
    s.addText(value, { x, y, w, h: 1.1, fontSize: 60, bold: true, color: color || C.accent1, fontFace: "+mj-lt", align: "left", valign: "bottom", margin: 0, isTextBox: true });
    s.addText(label, { x, y: y + 1.15, w, h: 0.8, fontSize: 15, color: C.text2, valign: "top", margin: 0, isTextBox: true });
  };
  t.code = (s, x, y, w, h, lines, fontSize = 14) => {
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x, y, w, h, rectRadius: 0.08, fill: { color: C.text1 }, line: { color: C.text1 }, objectName: "code block" });
    const runs = lines.map((l, i) => {
      const comment = l.startsWith("#") || l.startsWith("//");
      return { text: l, options: { color: comment ? C.accent5 : C.background1, breakLine: i < lines.length - 1 } };
    });
    s.addText(runs, { x: x + 0.25, y: y + 0.15, w: w - 0.5, h: h - 0.3, fontSize, fontFace: MONO, valign: "top", margin: 0, isTextBox: true, objectName: "code" });
  };
  t.bullets = (s, x, y, w, h, items, fontSize = 16, color) => {
    const runs = items.map((it, i) => {
      const rich = Array.isArray(it);
      return rich
        ? it.map((r, j) => ({ text: r.text, options: { ...(r.options || {}), bullet: j === 0 ? true : undefined, breakLine: j === it.length - 1 && i < items.length - 1 } }))
        : [{ text: it, options: { bullet: true, breakLine: i < items.length - 1 } }];
    }).flat();
    s.addText(runs, { x, y, w, h, fontSize, color: color || C.text1, valign: "top", margin: 0, isTextBox: true, paraSpaceAfter: 8 });
  };
  t.caption = (s, x, y, w, text, align = "left") =>
    s.addText(text, { x, y, w, h: 0.35, fontSize: 11, italic: true, color: C.text2, align, margin: 0, isTextBox: true });
  t.table = (s, x, y, w, rows, colW, opts = {}) => {
    const fs = opts.fontSize || 14;
    const data = rows.map((r, i) => r.map((cell, j) => {
      const o = typeof cell === "object" ? cell : { text: String(cell) };
      return {
        text: o.text,
        options: {
          bold: i === 0 || o.bold,
          color: i === 0 ? C.background1 : (o.color || C.text1),
          fill: { color: i === 0 ? C.text1 : (i % 2 ? C.background1 : C.background2) },
          align: j === 0 ? "left" : (opts.align || "right"),
          fontFace: o.mono ? MONO : undefined,
          fontSize: fs,
        },
      };
    }));
    s.addTable(data, { x, y, w, colW, border: { type: "none" }, margin: [4, 8, 4, 8], rowH: opts.rowH || 0.38, valign: "middle" });
  };
  return t;
}

const chartBase = (extra) => ({
  catAxisLabelColor: H.dk2, valAxisLabelColor: H.dk2,
  catAxisLabelFontFace: "+mn-lt", valAxisLabelFontFace: "+mn-lt", dataLabelFontFace: "+mn-lt",
  legendFontFace: "+mn-lt", titleFontFace: "+mn-lt",
  catAxisLabelFontSize: 12, valAxisLabelFontSize: 11, dataLabelFontSize: 11, legendFontSize: 12,
  valGridLine: { color: "DDE3EC", size: 0.75 }, catGridLine: { style: "none" },
  catAxisLineShow: false, valAxisLineShow: false,
  showLegend: true, legendPos: "b", legendColor: H.dk2,
  showTitle: true, titleColor: H.dk1, titleFontSize: 14,
  barGapWidthPct: 60,
  ...extra,
});

// Measured data (docs/PERFORMANCE.md, Tables 2 and 3: one node, job 19513).
const DATA = {
  metaCats: ["create", "stat", "unlink", "mkdir", "rmdir"],
  meta: { nfs: [67, 151, 181, 124, 153], cs: [25532, 592191, 84573, 37604, 86994], p2p: [26943, 42318, 40073, 35406, 40655], tmpfs: [272010, 690129, 680908, 364622, 656468] },
  sizes: ["1 MiB", "16 MiB", "128 MiB", "512 MiB"],
  write: { nfs: [54, 202, 411, 283], cs: [897, 1015, 637, 647], p2p: [1017, 972, 793, 841] },
  read: { nfs: [79, 115, 165, 117], cs: [884, 1298, 1121, 1186], p2p: [1296, 1282, 1355, 1387] },
  wfCats: ["8 × 128 MiB, shell tasks", "8 × 128 MiB, Python tasks", "2000 × 8 KiB files"],
  wf: { nfs: [64.4, 62.1, 257.6], cs: [33.9, 31.3, 24.7], p2p: [34.8, 39.4, 28.2], tmpfs: [15.6, 16.2, 15.0] },
};
const series = (obj, labels, keys) => keys.map((k) => ({ name: labels[k], labels: obj.cats, values: obj[k] }));
const NAMES = { nfs: "NFS (/home)", cs: "DAGonFS client-server", p2p: "DAGonFS peer-to-peer", tmpfs: "tmpfs (node-local bound)" };

async function blockPng() {
  const buf = await sharp(FIG("block-distribution.svg"), { density: 220 }).flatten({ background: "#FFFFFF" }).png().toBuffer();
  return "image/png;base64," + buf.toString("base64");
}

// ================================================================ scientific
async function scientific() {
  const { pres, C } = newDeck("DAGonFS: an in-memory ad-hoc file system for workflow scratch data", "DAGonFS · ad-hoc in-memory file system for workflow scratch data");
  const t = helpers(pres, C);
  const block = await blockPng();
  let s;

  // 1 — title
  pres.addSection({ title: "Introduction" });
  s = pres.addSlide({ masterName: "Title (dark)", sectionTitle: "Introduction" });
  s.addText("DAGonFS: an ad-hoc, in-memory, MPI-distributed file system for workflow scratch data", { placeholder: "title" });
  s.addText("One POSIX namespace over the aggregate RAM of an MPI job, for DAGonStar workflows on HPC systems", { placeholder: "subtitle" });
  s.addText("DAGonCAPIO research project · Università degli Studi di Napoli “Parthenope”\nWorkshop talk · 20 minutes", { placeholder: "meta" });
  s.addNotes("0:00–0:45. Introduce the problem in one sentence: workflow scratch data live on file systems built for durability. DAGonFS keeps them in the memory of the allocation instead. Outline: motivation, design, engineering assessment, evaluation with real DAGonStar workflows, limitations.");

  // 2 — motivation
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Introduction" });
  s.addText("Workflow scratch data pay for durability they never use", { placeholder: "title" });
  s.addImage({ path: FIG("MotivationDAGonFS.png"), x: 0.6, y: 1.45, w: 6.9, h: 4.49, altText: "Workflow tasks exchanging intermediate files through a shared persistent file system versus through DAGonFS" });
  t.caption(s, 0.6, 6.0, 6.9, "DAGonStar tasks exchange intermediate files through workflow://<task>/ references");
  t.point(s, 7.9, 1.45, 4.8, 1.55, 1, "Short-lived files", "Created once, read once by a downstream task, discarded when the workflow ends.");
  t.point(s, 7.9, 3.2, 4.8, 1.55, 2, "Shared storage is slow here", "Each create or stat is a round trip to the NFS server: 67 creates/s on our cluster.", { badge: C.accent3 });
  t.point(s, 7.9, 4.95, 4.8, 1.55, 3, "tmpfs is fast but private", "Node-local memory cannot be shared by the tasks of a multi-node workflow.", { badge: C.accent5 });
  s.addNotes("0:45–2:15. DAGonStar's workflow:// schema addresses the output of a task in its scratch directory; consumers stage it. On HPC systems that scratch sits on NFS or Lustre, so thousands of tiny metadata operations and short transfers pay the cost of durable storage. tmpfs is the speed bound, but private to a node. Goal: tmpfs-like behaviour with a namespace shared across nodes.");

  // 3 — answer up front
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Introduction" });
  s.addText("DAGonFS keeps the scratch space in the memory of the job", { placeholder: "title" });
  s.addText("A FUSE file system whose file contents are spread over the RAM of the MPI ranks. Real DAGonStar workflows, one node, versus the cluster's NFS home:", { x: 0.6, y: 1.4, w: 12.1, h: 0.8, fontSize: 18, color: C.text2, margin: 0, isTextBox: true });
  t.stat(s, 0.6, 2.4, 3.8, "1.9×", "faster: large-file dataflow\n8 × 128 MiB, shell tasks");
  t.stat(s, 4.75, 2.4, 3.8, "2.0×", "faster: the same dataflow\nwith Python tasks");
  t.stat(s, 8.9, 2.4, 3.8, "10.4×", "faster: small-file dataflow\n2000 × 8 KiB, 8 tasks", C.accent2);
  t.card(s, 0.6, 5.0, 12.1, 1.55);
  s.addText([
    { text: "47 defects found and fixed ", options: { bold: true, color: C.text1 } },
    { text: "in a systematic assessment. Both deployment models pass 30 POSIX scenarios, cross-peer replication and lock checks, and parallel MPI-IO (OMPIO and ROMIO), on one node and across two Slurm nodes.", options: { color: C.text2 } },
  ], { x: 0.9, y: 5.15, w: 11.5, h: 1.25, fontSize: 16, valign: "middle", margin: 0, isTextBox: true });
  s.addNotes("2:15–3:15. Give the result first. Medians of three runs (Slurm job 19513, PERFORMANCE.md Table 3). The gain is largest on the small-file workflow because it is metadata-bound, where NFS is weakest. Over two nodes the speed-ups stay at 1.9–10.3×.");

  // 4 — contributions
  pres.addSection({ title: "Design" });
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Design" });
  s.addText("Contributions", { placeholder: "title" });
  t.point(s, 0.6, 1.45, 5.9, 2.45, 1, "One binary, two run-time models", "Client-server or peer-to-peer is a property of the volume (--model, DAGonFS.ini, C++ and Python APIs), not of the build.");
  t.point(s, 6.8, 1.45, 5.9, 2.45, 2, "Point-to-point data plane", "128 KiB blocks laid out deterministically over the ranks; non-blocking transfers, no collectives, no address ever leaves its process.");
  t.point(s, 0.6, 4.15, 5.9, 2.45, 3, "Complete POSIX surface", "Every FUSE operation with a POSIX counterpart, including FIFOs, fallocate, SEEK_DATA/HOLE and cluster-wide fcntl, lockf and flock locks.");
  t.point(s, 6.8, 4.15, 5.9, 2.45, 4, "Assessment and evaluation", "47 defects with root causes and remedies; a benchmark with real DAGonStar workflows against NFS and tmpfs on a Slurm cluster.");
  s.addNotes("3:15–4:15. Four contributions. Emphasise that the model is chosen per volume at run time: the two models share all code and differ only in which ranks mount and whether namespace operations are replicated.");

  // 5 — architecture stack
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Design" });
  s.addText("Architecture: the same stack on every rank", { placeholder: "title" });
  const layers = [
    ["Workflow tasks", "POSIX calls, Python, shell tools", C.accent5],
    ["Linux VFS + FUSE", "/dev/fuse requests to user space", C.accent5],
    ["FileSystem", "FUSE low-level handlers, single reply each", C.accent1],
    ["Namespace · file buffer · lock table", "inodes, open-file image, locks on rank 0", C.accent1],
    ["Control plane · data plane", "PeerRequest messages · block slices + ACKs", C.accent4],
  ];
  layers.forEach(([head, sub, col], i) => {
    const y = 1.45 + i * 1.05;
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: 0.6, y, w: 6.6, h: 0.85, rectRadius: 0.1, fill: { color: col }, line: { color: col }, objectName: `layer ${head}` });
    s.addText([{ text: head, options: { bold: true, breakLine: true, fontSize: 16 } }, { text: sub, options: { fontSize: 13 } }], { x: 0.85, y, w: 6.1, h: 0.85, color: C.background1, valign: "middle", margin: 0, isTextBox: true });
  });
  s.addText("MPI point-to-point ⇄ other ranks", { x: 0.6, y: 6.55, w: 6.6, h: 0.35, fontSize: 13, italic: true, color: C.text2, align: "center", margin: 0, isTextBox: true });
  t.bullets(s, 7.7, 1.5, 5.0, 4.9, [
    [{ text: "Front-end buffer. ", options: { bold: true } }, { text: "An open file is a whole-file image in memory; reads and writes run at memory speed." }],
    [{ text: "Write-back on flush/close. ", options: { bold: true } }, { text: "The image is scattered to every rank; close returns after an ACK from each." }],
    [{ text: "Listener. ", options: { bold: true } }, { text: "Serves transfers, releases and lock requests; polls with MPI_Iprobe and a 0.5 ms back-off." }],
    [{ text: "No compile-time switches. ", options: { bold: true } }, { text: "A source-contract test enforces it mechanically." }],
  ], 16);
  s.addNotes("4:15–5:45. Walk the stack top to bottom. The front-end buffer is the key design choice: it turns many small FUSE writes into one distributed store at close. The listener never blocks in MPI_Probe: a blocking probe held the progress engine under MPI_THREAD_MULTIPLE and cost ~400 ms per operation across nodes (D33).");

  // 6 — models
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Design" });
  s.addText("Two deployment models, chosen per volume at run time", { placeholder: "title" });
  s.addImage({ path: FIG("CSArchitecture.jpg"), x: 0.6, y: 1.4, w: 5.9, h: 3.93, altText: "Client-server model: rank 0 mounts the volume, other ranks store blocks" });
  s.addImage({ path: FIG("P2PArchitecture.jpg"), x: 6.93, y: 1.35, w: 5.77, h: 3.99, altText: "Peer-to-peer model: every rank mounts the volume and replicates the namespace" });
  s.addText([{ text: "Client-server  ", options: { bold: true, color: C.accent1 } }, { text: "Rank 0 mounts <root> and owns the namespace; ranks 1…P−1 only store blocks. Linearizable for tasks on rank 0's node.", options: { color: C.text2 } }], { x: 0.6, y: 5.5, w: 5.9, h: 1.2, fontSize: 15, valign: "top", margin: 0, isTextBox: true });
  s.addText([{ text: "Peer-to-peer  ", options: { bold: true, color: C.accent4 } }, { text: "Every rank mounts <root>/<rank> and replicates names and metadata to the others; tasks on any node share one tree.", options: { color: C.text2 } }], { x: 6.93, y: 5.5, w: 5.77, h: 1.2, fontSize: 15, valign: "top", margin: 0, isTextBox: true });
  s.addNotes("5:45–7:00. Client-server: single FUSE loop on rank 0 serialises every request, so the kernel's 1 s attribute caches stay coherent. Peer-to-peer: every peer serves its own mount; namespace and metadata changes (names, links, modes, owners, times, xattrs) are replicated. MPI_THREAD_MULTIPLE is requested only for peer-to-peer volumes.");

  // 7 — data plane
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Design" });
  s.addText("Data plane: a deterministic, contiguous block layout", { placeholder: "title" });
  s.addImage({ data: block, x: 0.6, y: 1.5, w: 7.4, h: 3.21, altText: "Blocks of a file laid out contiguously over four ranks" });
  t.caption(s, 0.6, 4.8, 7.4, "A file of N blocks over P = 4 ranks: each rank stores one contiguous slice");
  t.card(s, 8.4, 1.5, 4.3, 1.5);
  s.addText([{ text: "N = ⌈S / B⌉,  B = 128 KiB", options: { bold: true, breakLine: true } }, { text: "content padded to whole blocks", options: { fontSize: 13, color: C.text2 } }], { x: 8.65, y: 1.5, w: 3.85, h: 1.5, fontSize: 17, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
  t.card(s, 8.4, 3.2, 4.3, 1.5);
  s.addText([{ text: "c_r = ⌊N/P⌋ + [r < N mod P]", options: { bold: true, breakLine: true } }, { text: "blocks on rank r, balanced within one", options: { fontSize: 13, color: C.text2 } }], { x: 8.65, y: 3.2, w: 3.85, h: 1.5, fontSize: 17, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
  t.bullets(s, 0.6, 5.35, 12.1, 1.4, [
    [{ text: "A reader needs only the stored size S ", options: { bold: true } }, { text: "to locate every block: no metadata exchange, no pointer ever crosses a process boundary." }],
    [{ text: "All MPI_Isend/MPI_Irecv are posted before any wait, ", options: { bold: true } }, { text: "so concurrent originators can neither mismatch nor deadlock (collectives would)." }],
  ], 15);
  s.addNotes("7:00–8:30. The layout is a pure function of (N, P). The original code exchanged raw block addresses and freed pointers belonging to other processes (D1); collectives rooted at different peers on one communicator mixed old and new blocks (D34). A linear point-to-point store costs about α(P−1) + β(P−1)/P·NB: only (P−1)/P of the data crosses the network.");

  // 8 — data path
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Design" });
  s.addText("Close-to-open data path", { placeholder: "title" });
  const steps = [
    ["open", "READ request; each rank sends its slice (LOAD)"],
    ["read / write", "served from the front-end buffer, any offset"],
    ["flush / close", "WRITE request; c_r blocks to rank r (STORE)"],
    ["store", "each rank replaces its slice, frees the old one"],
    ["ACK", "from every rank; then close returns"],
  ];
  steps.forEach(([head, body], i) => {
    const x = 0.6 + i * 2.48;
    t.card(s, x, 1.6, 2.2, 2.9);
    t.badge(s, x + 0.25, 1.85, i + 1, i < 2 ? C.accent1 : C.accent4);
    s.addText(head, { x: x + 0.25, y: 2.5, w: 1.8, h: 0.5, fontSize: 18, bold: true, color: C.text1, margin: 0, isTextBox: true });
    s.addText(body, { x: x + 0.25, y: 3.05, w: 1.8, h: 1.35, fontSize: 14, color: C.text2, valign: "top", margin: 0, isTextBox: true });
    if (i < steps.length - 1) s.addText("›", { x: x + 2.2, y: 2.7, w: 0.28, h: 0.6, fontSize: 28, bold: true, color: C.accent2, align: "center", valign: "middle", margin: 0, isTextBox: true });
  });
  t.bullets(s, 0.6, 4.9, 12.1, 1.8, [
    [{ text: "Consistency: ", options: { bold: true } }, { text: "any open that starts after close returns, on any peer where the name is visible, sees the new content." }],
    [{ text: "Cost: ", options: { bold: true } }, { text: "constant terms dominate small files, which is why DAGonFS synchronises at close/fsync rather than on every write." }],
    [{ text: "fsync is an ordering barrier, ", options: { bold: true } }, { text: "never a durability guarantee: the volume is volatile by design." }],
  ], 16);
  s.addNotes("8:30–9:30. One message per request (PeerRequest header + path bytes) on the request tag; block slices on dedicated STORE and LOAD tags; ACK on its own tag. Locks also order data: releasing a lock writes the file back first, and the next holder reloads a clean buffer if another peer changed it.");

  // 9 — peer-to-peer threads
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Design" });
  s.addText("Peer-to-peer: three threads avoid deadlock", { placeholder: "title" });
  const threads = [
    ["FUSE thread", "Serves the peer's own mount; originates transfers and broadcasts namespace changes.", C.accent1],
    ["Listener", "Serves other peers' transfers, releases and lock requests; never touches a mount point.", C.accent4],
    ["Replayer", "Applies remote namespace and metadata changes through the local mount, from a FIFO.", C.accent2],
  ];
  threads.forEach(([head, body, col], i) => {
    const x = 0.6 + i * 4.1;
    t.card(s, x, 1.5, 3.8, 2.4);
    t.badge(s, x + 0.25, 1.75, i + 1, col);
    s.addText(head, { x: x + 0.95, y: 1.72, w: 2.7, h: 0.55, fontSize: 18, bold: true, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
    s.addText(body, { x: x + 0.25, y: 2.45, w: 3.3, h: 1.35, fontSize: 15, color: C.text2, valign: "top", margin: 0, isTextBox: true });
  });
  t.card(s, 0.6, 4.2, 12.1, 2.45, C.background2);
  t.bullets(s, 0.9, 4.4, 11.5, 2.1, [
    [{ text: "Why: ", options: { bold: true } }, { text: "a replayed call is served by the peer's FUSE thread, which may be waiting on another listener. Listeners only do work that completes without the local kernel, so every wait-for chain ends at a listener that can progress (D35)." }],
    [{ text: "Cluster-wide file identity: ", options: { bold: true } }, { text: "gid = ((r_c + 1) ≪ 48) | ino_c, assigned at creation, so peers agree on a file although local inode numbers diverge (D20)." }],
    [{ text: "Replays are recognised by the replayer's thread id ", options: { bold: true } }, { text: "and never re-broadcast; kernel name and attribute caches are off (timeouts 0)." }],
  ], 15);
  s.addNotes("9:30–11:00. This is the subtle part of the design. If the listener issued replays itself, two peers writing at the same time could each wait for the other's listener while each listener waits for its own FUSE thread. Splitting off the replayer breaks the cycle. Namespace replication is asynchronous and ordered per originating peer by MPI's non-overtaking rule.");

  // 10 — assessment
  pres.addSection({ title: "Assessment" });
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Assessment" });
  s.addText("Assessment: 47 defects, crash to sub-ms", { placeholder: "title" });
  t.table(s, 0.6, 1.5, 5.6, [
    ["Defect class", "Count"],
    ["Data-plane memory safety (D1–D5)", "5"],
    ["FUSE protocol (D6–D8)", "3"],
    ["POSIX semantics (D9–D19)", "11"],
    ["Distributed protocol (D20–D26, D33–D40)", "15"],
    ["Found by the performance study (D41–D46)", "6"],
    ["Lifecycle, launcher, build (D27–D32, D47)", "7"],
    [{ text: "Total", bold: true }, { text: "47", bold: true }],
  ], [4.6, 1.0], { fontSize: 14 });
  s.addText("Peer-to-peer latency per operation, 2 nodes × 2 ranks", { x: 6.7, y: 1.5, w: 6.0, h: 0.38, fontSize: 14, bold: true, color: C.text1, margin: 0, isTextBox: true });
  t.table(s, 6.7, 1.95, 6.0, [
    ["Operation", "Before", "After"],
    ["mkdir / rmdir", "363 / 450 ms", { text: "0.43 / 0.03 ms", color: C.accent1, bold: true }],
    ["create / unlink", "427 / 415 ms", { text: "0.07 / 0.03 ms", color: C.accent1, bold: true }],
    ["write / read 4 KiB", "373 / 476 ms", { text: "0.26 / 0.20 ms", color: C.accent1, bold: true }],
  ], [2.2, 1.9, 1.9], { fontSize: 14 });
  t.bullets(s, 6.7, 3.75, 6.0, 2.9, [
    "Before: the client-server model crashed during the suite (cross-address-space free()).",
    "Every regression scenario was written before its fix, so it reproduces the defect.",
    "Removing the 400 ms latency exposed concurrency defects D34–D39 that it had masked.",
  ], 15);
  s.addNotes("11:00–12:30. Method: static review against the libfuse and MPI contracts, differential review of the two former source trees, dynamic testing on the production cluster. Highlight D1 (free() of another process's pointer) and D33 (blocking probe). The structural remedy was one source tree with the model as a run-time property.");

  // 11 — setup
  pres.addSection({ title: "Evaluation" });
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Evaluation" });
  s.addText("Evaluation setup", { placeholder: "title" });
  t.table(s, 0.6, 1.5, 6.4, [
    ["Item", "Value"],
    ["Nodes", "2 × Xeon Gold 5218, 187 GB, InfiniBand"],
    ["System", "Rocky Linux 8.10, libfuse 3.3.0"],
    ["MPI", "Open MPI 4.1.4, UCX 1.15"],
    ["DAGonFS", "4 ranks, Release, 128 KiB blocks"],
    ["Baselines", "NFS 4.2 /home; tmpfs /dev/shm"],
    ["Engine", "DAGonStar 1.0.2, Python 3.11"],
    ["Jobs", "19513 (1 node), 19514 (2 nodes)"],
  ], [1.6, 4.8], { align: "left", fontSize: 14 });
  t.point(s, 7.4, 1.5, 5.3, 1.6, 1, "Micro-benchmarks", "Metadata, 4 KiB files, 1–512 MiB sequential I/O; median of 5; page cache dropped before reads.", { bodySize: 14 });
  t.point(s, 7.4, 3.3, 5.3, 1.6, 2, "Large-file workflows", "8 × 128 MiB fan-out/fan-in, shell or Python tasks; ≈ 9 GiB written and read back.", { bodySize: 14 });
  t.point(s, 7.4, 5.1, 5.3, 1.6, 3, "Small-file workflow", "2000 × 8 KiB staged by 8 tasks; ≈ 18 000 creations. Median of 3, output verified.", { badge: C.accent2, bodySize: 14 });
  s.addNotes("12:30–13:30. Same benchmark program for every target; DAGonFS volumes created and destroyed through the Python API per target. The peer-to-peer volume is used through rank 0's mount, so it pays for replicating every operation to three peers. Every workflow output is checked; the Python reducer verifies every byte.");

  // 12 — metadata chart
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Evaluation" });
  s.addText("Metadata: two to three orders of magnitude above NFS", { placeholder: "title" });
  const metaObj = { cats: DATA.metaCats, ...DATA.meta };
  s.addChart(pres.charts.BAR, series(metaObj, NAMES, ["nfs", "cs", "p2p", "tmpfs"]), chartBase({
    x: 0.6, y: 1.4, w: 8.6, h: 5.3, barDir: "col", barGrouping: "clustered",
    chartColors: [SERIES.nfs, SERIES.cs, SERIES.p2p, SERIES.tmpfs],
    valAxisLogScaleBase: 10, valAxisMinVal: 10, valAxisLabelFormatCode: "#,##0",
    title: "Operations per second, 2000 files (log scale, median of 5)",
  }));
  t.stat(s, 9.6, 1.6, 3.1, "381×", "more creates per second than NFS (25 532 vs 67)");
  t.bullets(s, 9.6, 3.9, 3.1, 2.8, [
    "NFS: a server round trip per operation.",
    "Client-server stat nears tmpfs thanks to 1 s kernel caches.",
    "Peer-to-peer replicates every change: 17 000–43 000 ops/s.",
  ], 14);
  s.addNotes("13:30–14:45. Read the log axis carefully: NFS sustains 67–181 ops/s in this run; DAGonFS 25 000–590 000. Peer-to-peer disables the kernel caches because the listener changes names behind the kernel's back (D38), hence the lower stat rate.");

  // 13 — throughput
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Evaluation" });
  s.addText("Sequential I/O: 0.6–1.4 GiB/s on one node", { placeholder: "title" });
  const keys3 = ["nfs", "cs", "p2p"];
  s.addChart(pres.charts.BAR, series({ cats: DATA.sizes, ...DATA.write }, NAMES, keys3), chartBase({
    x: 0.6, y: 1.4, w: 6.0, h: 4.4, barDir: "col", chartColors: [SERIES.nfs, SERIES.cs, SERIES.p2p],
    valAxisMinVal: 0, valAxisMaxVal: 1500, valAxisMajorUnit: 500, title: "Write MiB/s (ends with fsync)", showLegend: false,
  }));
  s.addChart(pres.charts.BAR, series({ cats: DATA.sizes, ...DATA.read }, NAMES, keys3), chartBase({
    x: 6.7, y: 1.4, w: 6.0, h: 4.4, barDir: "col", chartColors: [SERIES.nfs, SERIES.cs, SERIES.p2p],
    valAxisMinVal: 0, valAxisMaxVal: 1500, valAxisMajorUnit: 500, title: "Read MiB/s (client cache dropped)", showLegend: false,
  }));
  [["NFS (/home)", SERIES.nfs], ["DAGonFS client-server", SERIES.cs], ["DAGonFS peer-to-peer", SERIES.p2p]].forEach(([n, col], i) => {
    const x = 2.6 + i * 2.9;
    s.addShape(pres.shapes.RECTANGLE, { x, y: 5.97, w: 0.22, h: 0.22, fill: { color: col }, line: { color: col }, objectName: `legend ${n}` });
    s.addText(n, { x: x + 0.32, y: 5.9, w: 2.5, h: 0.36, fontSize: 13, color: C.text2, valign: "middle", margin: 0, isTextBox: true });
  });
  s.addText("tmpfs reaches 2.8–5.5 GiB/s: FUSE crossings, whole-file buffering and one extra copy to distribute the data keep DAGonFS at a fifth to a third of it.", { x: 0.6, y: 6.35, w: 12.1, h: 0.55, fontSize: 14, color: C.text2, margin: 0, isTextBox: true });
  s.addNotes("14:45–15:45. NFS write rate rises with size because it writes back asynchronously until the final fsync; its reads, 79–165 MiB/s, reflect the server once the client cache is dropped. DAGonFS is 2–17× NFS here. The gap to tmpfs is the price of a user-space, distributed file system.");

  // 14 — workflows
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Evaluation" });
  s.addText("Real DAGonStar workflows: 1.9× to 10.4× faster than NFS", { placeholder: "title" });
  s.addChart(pres.charts.BAR, series({ cats: DATA.wfCats, ...DATA.wf }, NAMES, ["nfs", "cs", "p2p", "tmpfs"]), chartBase({
    x: 0.6, y: 1.4, w: 8.4, h: 5.3, barDir: "bar", chartColors: [SERIES.nfs, SERIES.cs, SERIES.p2p, SERIES.tmpfs],
    showValue: true, dataLabelPosition: "outEnd", dataLabelColor: H.dk2, dataLabelFormatCode: "0.0",
    valAxisMinVal: 0, valAxisMaxVal: 300, valAxisMajorUnit: 50, catAxisOrientation: "maxMin",
    title: "Wall time in seconds, median of 3 (lower is better)",
  }));
  t.card(s, 9.4, 1.5, 3.3, 5.15);
  s.addText("Speed-up over NFS, client-server", { x: 9.65, y: 1.65, w: 2.8, h: 0.6, fontSize: 14, bold: true, color: C.text1, margin: 0, isTextBox: true });
  [["1.9×", "shell tasks"], ["2.0×", "Python tasks"], ["10.4×", "small files"]].forEach(([v, l], i) => {
    s.addText(v, { x: 9.65, y: 2.35 + i * 1.12, w: 1.6, h: 0.75, fontSize: 36, bold: true, color: i === 2 ? C.accent2 : C.accent1, fontFace: "+mj-lt", valign: "middle", margin: 0, isTextBox: true });
    s.addText(l, { x: 11.25, y: 2.35 + i * 1.12, w: 1.3, h: 0.75, fontSize: 14, color: C.text2, valign: "middle", margin: 0, isTextBox: true });
  });
  s.addText("Two nodes: 1.9–10.3×. Peer-to-peer costs 0–26 % over client-server.", { x: 9.65, y: 5.7, w: 2.85, h: 0.85, fontSize: 13, color: C.text2, valign: "top", margin: 0, isTextBox: true });
  s.addNotes("15:45–17:00. The headline result. DAGonStar polls task state with fixed sleeps, so even an empty workflow takes about 7 s; discounting that floor DAGonFS spends about 18 s on the small-file workflow against about 250 s on NFS. Spreading the volume over two nodes did not slow the workflows, and tasks on either node see the same tree.");

  // 15 — limitations
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Evaluation" });
  s.addText("Limitations and threats to validity", { placeholder: "title" });
  t.card(s, 0.6, 1.5, 5.9, 5.15);
  s.addText("Design limitations", { x: 0.9, y: 1.7, w: 5.3, h: 0.5, fontSize: 20, bold: true, color: C.text1, margin: 0, isTextBox: true });
  t.bullets(s, 0.9, 2.35, 5.3, 4.2, [
    "Volatile: no replica, log or checkpoint; a rank failure loses its blocks.",
    "A file open on a front end must fit in that process's memory.",
    "Unsynchronised writers to one file: last write-back wins; use locks.",
    "One lock coordinator (rank 0); homogeneous ranks.",
  ], 17);
  t.card(s, 6.8, 1.5, 5.9, 5.15);
  s.addText("Threats to validity", { x: 7.1, y: 1.7, w: 5.3, h: 0.5, fontSize: 20, bold: true, color: C.text1, margin: 0, isTextBox: true });
  t.bullets(s, 7.1, 2.35, 5.3, 4.2, [
    "Shared NFS server of unknown load: its metadata rates varied 3–4× between runs.",
    "DAGonFS medians varied −9 % to +31 % between runs; treat differences under ~30 % as noise.",
    "NFS reads may hit the server's memory: an upper bound for cold data.",
    "Volatile versus durable: the comparison concerns discarded scratch data only.",
  ], 17);
  s.addNotes("17:00–18:00. Be explicit: this is scratch space, not storage. The repeatability study (PERFORMANCE.md §4.5) ran the same benchmark twice four hours apart and an A/B test of the HCOLL fix; compare configurations within one run.");

  // 16 — conclusions
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Evaluation" });
  s.addText("Conclusions and next steps", { placeholder: "title" });
  t.point(s, 0.6, 1.5, 5.9, 2.4, 1, "A shared, fast scratch space", "One POSIX namespace over the job's RAM: 1.9–2.0× faster than NFS on large-file workflows, 10.4× on small files.");
  t.point(s, 0.6, 4.15, 5.9, 2.5, 2, "Validated, not only measured", "47 defects fixed; 30 POSIX scenarios, replication, cluster-wide locks and MPI-IO pass on one and two Slurm nodes.");
  t.card(s, 6.8, 1.5, 5.9, 5.15, C.text1);
  s.addText("Next steps", { x: 7.1, y: 1.7, w: 5.3, h: 0.5, fontSize: 20, bold: true, color: C.accent2, margin: 0, isTextBox: true });
  t.bullets(s, 7.1, 2.35, 5.3, 4.1, [
    "Partial (block-range) front-end buffers for files larger than one node's memory.",
    "Distribute the lock coordinator.",
    "Optional block replication to survive a rank failure.",
    "MPI-IO processes on several nodes; larger node counts.",
  ], 17, C.background1);
  s.addNotes("18:00–19:15. Restate the two take-aways. The next steps follow directly from the limitations on the previous slide; present them as directions, not commitments.");

  // 17 — thanks
  s = pres.addSlide({ masterName: "Closing (dark)", sectionTitle: "Evaluation" });
  s.addText("Thank you. Questions?", { placeholder: "title" });
  s.addText([
    { text: "Code and documentation", options: { bold: true, color: C.accent2, breakLine: true } },
    { text: "github.com/DagOnStar/DAGonFS  ·  github.com/DagOnStar/dagonstar", options: { color: C.background1, breakLine: true } },
    { text: " ", options: { breakLine: true } },
    { text: "Earlier work", options: { bold: true, color: C.accent2, breakLine: true } },
    { text: "C. G. De Vita et al., An ad-hoc file system accelerated workflow application for accidental fire fast response, WiDE ’24, ACM, doi:10.1145/3642978.3652836", options: { color: C.background1 } },
  ], { x: 0.8, y: 2.0, w: 11.7, h: 2.9, fontSize: 16, valign: "top", margin: 0, isTextBox: true });
  s.addText("DAGonFS is part of the DAGonCAPIO project (CUP J33C22001170001), funded under the PNRR ICSC National Centre for HPC, Big Data and Quantum Computing, Spoke 1 “Future HPC and Big Data” cascade call.", { x: 0.8, y: 5.4, w: 11.7, h: 1.0, fontSize: 13, color: C.background2, valign: "top", margin: 0, isTextBox: true });
  s.addNotes("19:15–20:00. Questions. Likely ones: why not a parallel file system's burst buffer (needs no admin rights, lives with the job); fault tolerance (none by design); scaling beyond two nodes (next step).");

  const file = path.join(OUT, "DAGonFS-scientific-talk.pptx");
  await pres.writeFile({ fileName: file });
  if (applyTheme) await applyTheme(file, THEME);
  return file;
}

// ================================================================ tutorial
async function tutorial() {
  const { pres, C } = newDeck("DAGonFS hands-on tutorial", "DAGonFS tutorial · compile · install · configure · test · evaluate");
  const t = helpers(pres, C);
  const STEPS = ["Compile", "Install", "Configure", "Test", "Evaluate"];
  // Progress tracker: the tutorial's motif, top right of every step slide.
  const progress = (s, cur) => STEPS.forEach((name, i) => {
    const x = 8.35 + i * 0.88;
    const on = i === cur, done = i < cur;
    s.addShape(pres.shapes.OVAL, { x: x + 0.24, y: 0.42, w: 0.36, h: 0.36, fill: { color: on ? C.accent2 : (done ? C.accent1 : C.background2) }, line: { color: on ? C.accent2 : (done ? C.accent1 : C.background2) }, objectName: `step ${name}` });
    s.addText(String(i + 1), { x: x + 0.24, y: 0.42, w: 0.36, h: 0.36, fontSize: 12, bold: true, color: on || done ? C.background1 : C.text2, align: "center", valign: "middle", margin: 0, isTextBox: true });
    s.addText(name, { x, y: 0.82, w: 0.84, h: 0.28, fontSize: 10, bold: on, color: on ? C.text1 : C.text2, align: "center", margin: 0, isTextBox: true });
  });
  const titleOpts = { placeholder: "title" };
  const content = (section, title, step) => {
    const s = pres.addSlide({ masterName: step === undefined ? "Content" : "Content (step)", sectionTitle: section });
    s.addText(title, titleOpts);
    if (step !== undefined) progress(s, step);
    return s;
  };
  let s;

  // 1 — title
  pres.addSection({ title: "Overview" });
  s = pres.addSlide({ masterName: "Title (dark)", sectionTitle: "Overview" });
  s.addText("DAGonFS hands-on tutorial", { placeholder: "title" });
  s.addText("Compile, install, configure, test and evaluate an in-memory MPI file system for workflow scratch data", { placeholder: "subtitle" });
  s.addText("Tutorial · 20 minutes · Linux with FUSE 3, MPI and CMake\nRepository: github.com/DagOnStar/DAGonFS", { placeholder: "meta" });
  s.addNotes("0:00–0:30. Audience: users who will run DAGonFS on a workstation or a Slurm cluster. Every command on these slides is from docs/CONFIGURATION.md, docs/TESTING.md and docs/PERFORMANCE.md; expected results are the ones recorded there in November 2026.");

  // 2 — agenda
  s = content("Overview", "What we will do in 20 minutes");
  const agenda = [
    ["Compile", "CMake build, one dagonfs binary", "3 min"],
    ["Install", "relocatable tree, man page", "2 min"],
    ["Configure", "model, CLI, ini, launcher, Python", "5 min"],
    ["Test", "ctest, mounted suite, workflow, Slurm", "5 min"],
    ["Evaluate", "benchmark versus NFS and tmpfs", "4 min"],
  ];
  agenda.forEach(([head, body, min], i) => {
    const x = 0.6 + i * 2.48;
    t.card(s, x, 1.6, 2.2, 3.4);
    t.badge(s, x + 0.25, 1.85, i + 1, i === 4 ? C.accent2 : C.accent1, 0.6, 18);
    s.addText(head, { x: x + 0.25, y: 2.65, w: 1.8, h: 0.5, fontSize: 20, bold: true, color: C.text1, margin: 0, isTextBox: true });
    s.addText(body, { x: x + 0.25, y: 3.2, w: 1.8, h: 1.0, fontSize: 14, color: C.text2, valign: "top", margin: 0, isTextBox: true });
    s.addText(min, { x: x + 0.25, y: 4.35, w: 1.8, h: 0.45, fontSize: 14, bold: true, color: C.accent4, margin: 0, isTextBox: true });
  });
  s.addText([
    { text: "What DAGonFS is: ", options: { bold: true, color: C.text1 } },
    { text: "a FUSE file system that spreads file contents over the RAM of the ranks of an MPI job. It is volatile scratch space for workflows: stage results to persistent storage before you unmount it.", options: { color: C.text2 } },
  ], { x: 0.6, y: 5.35, w: 12.1, h: 1.2, fontSize: 16, valign: "top", margin: 0, isTextBox: true });
  s.addNotes("0:30–1:30. Five steps; the tracker at the top right of each slide shows where we are. Stress volatility up front: fsync orders operations inside the service, it does not persist anything.");

  // 3 — models
  s = content("Overview", "Two models, one executable");
  t.table(s, 0.6, 1.5, 12.1, [
    ["", "Client-server", "Peer-to-peer"],
    ["Select with", { text: "--model client-server (default)", mono: true }, { text: "--model peer-to-peer", mono: true }],
    ["Mount points", { text: "<root> on rank 0", mono: true }, { text: "<root>/<rank> on every rank", mono: true }],
    ["Namespace", "held by rank 0; ranks 1…P−1 store blocks", "replicated on every peer"],
    ["Tasks run", "on rank 0's node", "on any node, using a local peer's mount"],
    ["MPI thread level", "MPI_THREAD_SINGLE", "MPI_THREAD_MULTIPLE"],
  ], [2.3, 4.9, 4.9], { align: "left", fontSize: 15, rowH: 0.5 });
  t.point(s, 0.6, 4.85, 12.1, 1.75, "?", "Which one should I use?", "Client-server when every workflow task runs on one node; peer-to-peer when tasks run on several nodes and must see the same tree. DAGonFS_CS.exe and DAGonFS_P2P.exe remain as links that only set the default model.", { badge: C.accent2, bodySize: 15 });
  s.addNotes("1:30–2:30. The model is a run-time property of the volume, not of the build: no recompilation to switch. Client-server is linearizable for tasks on node 0; peer-to-peer replicates names and metadata asynchronously and gives close-to-open data consistency.");

  // 4 — prerequisites
  pres.addSection({ title: "Compile" });
  s = content("Compile", "Prerequisites", 0);
  t.table(s, 0.6, 1.5, 7.6, [
    ["Component", "Requirement"],
    ["Build system", "CMake ≥ 3.20 (network once, for log4cplus)"],
    ["Compiler", "C++23: GCC ≥ 12 or Clang ≥ 16"],
    ["MPI", "MPI-3; MPI_THREAD_MULTIPLE for peer-to-peer"],
    ["FUSE", "libfuse ≥ 3.3, /dev/fuse, set-uid fusermount3"],
    ["Python", "3.x for tests; DAGonStar for workflow tests"],
  ], [2.0, 5.6], { align: "left", fontSize: 15, rowH: 0.5 });
  t.code(s, 8.6, 1.5, 4.1, 2.3, [
    "# check the host",
    "cmake --version",
    "mpirun --version",
    "pkg-config --modversion fuse3",
    "ls -l /dev/fuse",
    "which fusermount3",
  ], 13);
  t.bullets(s, 8.6, 4.1, 4.1, 2.5, [
    "Tested: Open MPI 4.1.4, libfuse 3.3.0, GCC 12.2.",
    "Only FUSE 2's fusermount? tests/slurm/fuse-env.sh links it as fusermount3.",
  ], 14);
  s.addNotes("2:30–3:15. On RHEL/Rocky 8 libfuse is 3.3: the lseek and copy_file_range callbacks are then not compiled and the kernel serves those calls itself, with the same results. On clusters, load the MPI module first.");

  // 5 — build
  s = content("Compile", "Build and smoke-test", 0);
  t.code(s, 0.6, 1.5, 7.2, 1.75, [
    "git clone https://github.com/DagOnStar/DAGonFS.git",
    "cd DAGonFS",
    "cmake -S . -B build -DUSE_MPI=ON -DCMAKE_BUILD_TYPE=Release",
    "cmake --build build --parallel",
    "ctest --test-dir build --output-on-failure",
  ], 13);
  s.addText("Expected", { x: 0.6, y: 3.5, w: 7.2, h: 0.4, fontSize: 16, bold: true, color: C.accent1, margin: 0, isTextBox: true });
  t.code(s, 0.6, 3.95, 7.2, 1.8, [
    "dagonfs-source-contract ....   Passed",
    "dagonfs-usage ..............   Passed",
    "dagonfs-lock-table .........   Passed",
    "dagonfs-launcher-dry-run ...   Passed",
    "100% tests passed, 0 tests failed out of 4",
  ], 13);
  s.addText("Build products in build/", { x: 8.2, y: 1.5, w: 4.5, h: 0.4, fontSize: 16, bold: true, color: C.text1, margin: 0, isTextBox: true });
  t.table(s, 8.2, 2.0, 4.5, [
    ["File", "Purpose"],
    [{ text: "dagonfs", mono: true }, "both models"],
    [{ text: "DAGonFS_Launcher", mono: true }, "ini-driven mpirun"],
    [{ text: "DAGonFS_CS.exe", mono: true }, "alias, client-server"],
    [{ text: "DAGonFS_P2P.exe", mono: true }, "alias, peer-to-peer"],
    [{ text: "dagonfs_mpiio_check", mono: true }, "MPI-IO test client"],
  ], [2.4, 2.1], { align: "left", fontSize: 13, rowH: 0.42 });
  s.addText("Compiled with -Wall -Wextra; the source contract needs no FUSE.", { x: 8.2, y: 4.85, w: 4.5, h: 0.8, fontSize: 14, color: C.text2, valign: "top", margin: 0, isTextBox: true });
  s.addNotes("3:15–5:00. The first configure downloads and builds log4cplus, so it needs network access once. ctest runs four tests: the source-contract test (architectural invariants), the usage test, the lock-table unit tests, and a launcher dry run. None needs a mount.");

  // 6 — install
  pres.addSection({ title: "Install" });
  s = content("Install", "Install a relocatable tree", 1);
  t.code(s, 0.6, 1.5, 6.6, 1.35, [
    "cmake --install build --prefix \"$HOME/opt/dagonfs\"",
    "export PATH=\"$HOME/opt/dagonfs/bin:$PATH\"",
    "man docs/man/dagonfs.1        # without installing",
  ], 13);
  t.table(s, 0.6, 3.1, 6.6, [
    ["Path under the prefix", "Content"],
    [{ text: "bin/", mono: true }, "dagonfs, launcher, the two aliases"],
    [{ text: "lib/", mono: true }, "log4cplus, static volume libraries"],
    [{ text: "include/dagonfs/", mono: true }, "Options.hpp, Volume.hpp"],
    [{ text: "share/man/man1/", mono: true }, "dagonfs.1, DAGonFS_Launcher.1"],
    [{ text: "share/doc/DAGonFS/", mono: true }, "the Markdown documentation"],
  ], [2.6, 4.0], { align: "left", fontSize: 14, rowH: 0.45 });
  t.point(s, 7.6, 1.5, 5.1, 1.65, 1, "Relocatable", "Relative RPATH to lib/: copy the whole tree to compute nodes as a unit.", { bodySize: 14 });
  t.point(s, 7.6, 3.35, 5.1, 1.65, 2, "Same path everywhere", "Executable, ini file and mount root must be valid on every node.", { bodySize: 14 });
  t.point(s, 7.6, 5.2, 5.1, 1.45, 3, "FUSE on every node", "Select node classes that provide /dev/fuse.", { badge: C.accent2, bodySize: 14 });
  s.addNotes("5:00–6:30. Installation is optional for a workstation (run from build/). On a cluster, stage the installed tree with the scheduler or put it on a shared file system. The C++ API is the static library dagonfs_core with include/dagonfs/Volume.hpp.");

  // 7 — CLI
  pres.addSection({ title: "Configure" });
  s = content("Configure", "Create a volume from the command line", 2);
  t.code(s, 0.6, 1.5, 12.1, 1.3, [
    "mpirun -np 4 build/dagonfs --model client-server -f /tmp/dagonfs   # mounted at /tmp/dagonfs",
    "mpirun -np 4 build/dagonfs --model peer-to-peer  -f /tmp/dagonfs   # /tmp/dagonfs/0 … /3",
    "mpirun -np 4 build/dagonfs --config DAGonFS.ini                    # model and root from ini",
  ], 14);
  t.table(s, 0.6, 3.5, 7.4, [
    ["Option", "Meaning"],
    [{ text: "-m, --model M", mono: true }, "client-server (cs) | peer-to-peer (p2p)"],
    [{ text: "-c, --config F", mono: true }, "read model, root, mode, logging"],
    [{ text: "-f / -d", mono: true }, "foreground / with FUSE trace"],
    [{ text: "--log", mono: true }, "verbose log4cplus output"],
    [{ text: "<root>", mono: true }, "mount point or parent of mounts"],
  ], [2.4, 5.0], { align: "left", fontSize: 14, rowH: 0.45 });
  t.card(s, 8.4, 3.5, 4.3, 3.15);
  s.addText("Precedence, low → high", { x: 8.65, y: 3.65, w: 3.8, h: 0.45, fontSize: 16, bold: true, color: C.text1, margin: 0, isTextBox: true });
  ["built-in defaults", "program name (alias)", "--config file", "explicit flags"].forEach((p, i) => {
    t.badge(s, 8.65, 4.2 + i * 0.58, i + 1, C.accent4, 0.42, 13);
    s.addText(p, { x: 9.2, y: 4.2 + i * 0.58, w: 3.3, h: 0.42, fontSize: 15, color: C.text2, valign: "middle", margin: 0, isTextBox: true });
  });
  s.addNotes("6:30–7:45. Mount points and missing parents are created on each node. Startup failures (a non-empty mount point, a missing /dev/fuse) are always printed with the rank number. Expected after mounting: `mount | grep dagonfs` shows the FUSE mount(s), and `ls` of the mount point works.");

  // 8 — ini + launcher
  s = content("Configure", "DAGonFS.ini and the launcher", 2);
  t.code(s, 0.6, 1.5, 6.3, 3.6, [
    "[DAGonFS_config]",
    "num_processes = 8",
    "machinefile =",
    "dagonfs_model = client-server",
    "fuse_mode = f",
    "dagonfs_root_dir = /tmp/DAGonFS",
    "enable_logging = false",
    "mpirun = mpirun",
    "executable =",
  ], 14);
  t.code(s, 7.3, 1.5, 5.4, 1.2, [
    "cd build && cp ../src/DAGonFS.ini .",
    "./DAGonFS_Launcher --model peer-to-peer \\",
    "    --dry-run",
  ], 13);
  s.addText("Expected", { x: 7.3, y: 2.95, w: 5.4, h: 0.4, fontSize: 16, bold: true, color: C.accent1, margin: 0, isTextBox: true });
  t.code(s, 7.3, 3.4, 5.4, 0.9, [
    "Executing: mpirun -np 8 ./dagonfs",
    "  --model peer-to-peer -f /tmp/DAGonFS",
  ], 13);
  t.bullets(s, 7.3, 4.6, 5.4, 2.0, [
    "Runs mpirun with execvp, no shell: values are passed literally.",
    "mpirun may be srun or mpiexec; machinefile adds --hostfile.",
  ], 14);
  s.addText("num_processes and dagonfs_root_dir are required; --model overrides dagonfs_model.", { x: 0.6, y: 5.35, w: 6.3, h: 0.9, fontSize: 15, color: C.text2, valign: "top", margin: 0, isTextBox: true });
  s.addNotes("7:45–9:00. The launcher validates the ini file before running anything. Drop --dry-run to start the volume. The same keys (model, root, fuse_mode, enable_logging) are read by `dagonfs --config`.");

  // 9 — Python + DAGonStar
  s = content("Configure", "From Python and DAGonStar", 2);
  t.code(s, 0.6, 1.5, 7.3, 3.2, [
    "import sys; sys.path.insert(0, \"python\")",
    "from dagonfs import Volume",
    "",
    "with Volume(\"/tmp/dagonfs\", model=\"peer-to-peer\",",
    "            ranks=4, executable=\"build/dagonfs\",",
    "            local_ranks=(0,)) as volume:",
    "    scratch = volume.mountpoint(0)   # /tmp/dagonfs/0",
    "    ...  # point DAGonStar's scratch_dir_base here",
    "# leaving the block unmounts and waits for the job",
  ], 13);
  t.point(s, 8.3, 1.5, 4.4, 1.55, 1, "start()", "Returns once the local mount points are mounted.", { bodySize: 14 });
  t.point(s, 8.3, 3.2, 4.4, 1.55, 2, "stop()", "Unmounts them, shutting the volume down; returns the job status.", { bodySize: 14 });
  t.point(s, 0.6, 4.95, 12.1, 1.7, 3, "With DAGonStar", "Start DAGonFS before DAGonStar creates task directories, put the scratch root on the mount, and stage final outputs to persistent storage before the volume ends. workflow:// references are resolved by DAGonStar, not by DAGonFS.", { badge: C.accent2, bodySize: 14 });
  s.addNotes("9:00–10:00. The module uses only the standard library. local_ranks lists the ranks placed on the calling host; pass mpirun_args such as ['--hostfile', 'hosts'] for multi-host volumes. In peer-to-peer mode a consumer on another node should rely on DAGonStar's dependencies, since namespace changes arrive asynchronously (milliseconds).");

  // 10 — shutdown & troubleshooting
  s = content("Configure", "Shutdown and troubleshooting", 2);
  t.code(s, 0.6, 1.5, 5.6, 1.0, [
    "fusermount3 -u /tmp/DAGonFS     # client-server",
    "fusermount3 -u /tmp/DAGonFS/0   # p2p: any peer",
  ], 13);
  s.addText("Unmounting one front end stops the whole volume: TERMINATE reaches every rank, which unmounts and exits.", { x: 0.6, y: 2.75, w: 5.6, h: 1.0, fontSize: 15, color: C.text2, valign: "top", margin: 0, isTextBox: true });
  t.table(s, 6.6, 1.5, 6.1, [
    ["Symptom", "Fix"],
    ["Transport endpoint is not connected", "job was killed: fusermount3 -uz <mount>"],
    ["Job hangs in MPI_Finalize", "HCOLL is disabled by default (D47)"],
    ["Rank killed during a workflow", "request --mem or --exclusive"],
    ["mpirun not found in sbatch", "load the MPI module before sbatch"],
    ["Mount fails on a node", "node lacks /dev/fuse: pick a partition"],
  ], [2.9, 3.2], { align: "left", fontSize: 13, rowH: 0.5 });
  t.card(s, 0.6, 4.0, 5.6, 1.9);
  s.addText([
    { text: "Memory sizing per rank", options: { bold: true, color: C.text1, breakLine: true } },
    { text: "≈ 1/P of the scratch data, plus the whole content of every file open on that front end, plus MPI and FUSE buffers.", options: { color: C.text2 } },
  ], { x: 0.85, y: 4.15, w: 5.1, h: 1.6, fontSize: 15, valign: "top", margin: 0, isTextBox: true });
  s.addNotes("10:00–11:00. Never kill the MPI job to stop DAGonFS: unmount. Slurm's default 1 GB per CPU on our cluster made the OOM killer stop a rank during the first large workflow; the benchmark jobs requested --mem=96G. Set OMPI_MCA_coll_hcoll_enable=1 only if you need HCOLL back.");

  // 11 — validation ladder
  pres.addSection({ title: "Test" });
  s = content("Test", "Validation ladder", 3);
  const ladder = [
    ["Source contract", "python3 tests/test_source_contract.py", "any machine"],
    ["Build and smoke", "ctest --test-dir build", "compiler, MPI, FUSE headers"],
    ["Mounted suite", "python3 tests/mount.py --model … --ranks 4", "Linux, /dev/fuse"],
    ["Workflow", "python3 tests/dagonstar/run_workflow.py …", "+ DAGonStar"],
    ["Multi-node", "sbatch -N 2 … tests/slurm/multinode.sbatch build", "Slurm, FUSE nodes"],
  ];
  ladder.forEach(([head, cmd, req], i) => {
    const y = 1.45 + i * 1.03;
    t.badge(s, 0.6, y + 0.17, i + 1, i < 2 ? C.accent5 : (i < 4 ? C.accent1 : C.accent4));
    t.card(s, 1.3, y, 11.4, 0.85);
    s.addText(head, { x: 1.55, y, w: 2.3, h: 0.85, fontSize: 16, bold: true, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
    s.addText(cmd, { x: 3.9, y, w: 6.2, h: 0.85, fontSize: 13, fontFace: MONO, color: C.text1, valign: "middle", margin: 0, isTextBox: true });
    s.addText(req, { x: 10.2, y, w: 2.35, h: 0.85, fontSize: 13, color: C.text2, valign: "middle", margin: 0, isTextBox: true });
  });
  s.addNotes("11:00–12:15. Climb the ladder: each level is costlier and closer to production. The first two run in CI on every push. The mounted harness always unmounts, even when a test fails; arguments after `--` go to mpirun (e.g. --oversubscribe on a laptop).");

  // 12 — mounted tests
  s = content("Test", "Mounted suite and workflow test", 3);
  t.code(s, 0.6, 1.5, 7.4, 1.8, [
    "python3 tests/mount.py --build-dir build \\",
    "    --model peer-to-peer --ranks 4 -- --oversubscribe",
    "python3 tests/dagonstar/run_workflow.py \\",
    "    --build-dir build --model peer-to-peer \\",
    "    --ranks 4 -- --oversubscribe",
  ], 13);
  s.addText("Expected", { x: 0.6, y: 3.55, w: 7.4, h: 0.4, fontSize: 16, bold: true, color: C.accent1, margin: 0, isTextBox: true });
  t.code(s, 0.6, 4.0, 7.4, 1.8, [
    "Ran 30 tests in …s   OK",
    "peer-to-peer replication checks: OK",
    "MPI-IO checks: OK          # OMPIO and ROMIO",
    "workflow: 8 tasks finished in ~8 s; result {'ok': True, …}",
    "# then: no mount point left behind",
  ], 13);
  t.bullets(s, 8.4, 1.5, 4.3, 5.2, [
    [{ text: "30 POSIX scenarios: ", options: { bold: true } }, { text: "lifecycle, rename, links, xattrs, locks, fallocate, FIFOs, sockets." }],
    [{ text: "Peer-to-peer checks: ", options: { bold: true } }, { text: "cross-peer data, concurrent writers, metadata replication, locked counter = 80." }],
    [{ text: "MPI-IO: ", options: { bold: true } }, { text: "independent, collective, data sieving, atomic, shared pointer." }],
    [{ text: "Workflow: ", options: { bold: true } }, { text: "Python tasks run from the volume (≈ 8 s at width 6)." }],
  ], 16);
  s.addNotes("12:15–13:30. Run both models. Expected numbers from docs/TESTING.md Table 0c: 30/30 at 1–4 ranks, 12 MPI-IO results per client-server run and 14 per peer-to-peer run, workflow 8/8 tasks at width 6 and 18/18 at width 16 in 7.6–8.3 s. Point the suite at any mount with DAGONFS_MOUNTPOINT=/path python3 tests/test_python_posix.py.");

  // 13 — multi-node
  s = content("Test", "Multi-node validation with Slurm", 3);
  t.code(s, 0.6, 1.5, 12.1, 1.0, [
    "module load <MPI module>      # sbatch exports this environment",
    "sbatch -p <partition> -N 2 --ntasks-per-node=2 tests/slurm/multinode.sbatch build [python]",
  ], 14);
  t.point(s, 0.6, 3.05, 5.9, 1.7, 1, "What it runs", "For each model: the mounted suite, then the DAGonStar workflow with 8 Python tasks if the interpreter imports DAGonStar.", { bodySize: 14 });
  t.point(s, 0.6, 4.95, 5.9, 1.7, 2, "Rank placement", "Ranks 0 and 1 on the first node (--map-by slot); data blocks on every node.", { bodySize: 14 });
  t.table(s, 6.8, 3.05, 5.9, [
    ["Expected (jobs 19515, 19516)", "Result"],
    ["POSIX suite, both models", { text: "30/30 pass", color: C.accent1, bold: true }],
    ["Peer-to-peer checks", { text: "pass", color: C.accent1, bold: true }],
    ["MPI-IO results", { text: "26/26", color: C.accent1, bold: true }],
    ["Workflow, 8 Python tasks", { text: "10/10 tasks", color: C.accent1, bold: true }],
    ["Exit status", { text: "0 (non-zero if any run fails)", color: C.accent1, bold: true }],
  ], [3.3, 2.6], { align: "left", fontSize: 14, rowH: 0.5 });
  s.addNotes("13:30–14:30. Use only nodes that provide /dev/fuse, and never cancel jobs you did not submit. On our cluster the low-wn partition was used; the workflow took 13.0–14.2 s across nodes. Record your results in docs/TESTING.md.");

  // 14 — benchmark how-to
  pres.addSection({ title: "Evaluate" });
  s = content("Evaluate", "Run the benchmark", 4);
  t.code(s, 0.6, 1.5, 12.1, 1.85, [
    "python3.11 -m venv venv && venv/bin/pip install matplotlib dagonstar",
    "sbatch -p <partition> -N 1 -n 4 --exclusive --mem=96G bench/slurm/bench.sbatch build venv/bin/python",
    "sbatch -p <partition> -N 2 --ntasks-per-node=2 --exclusive --mem=96G \\",
    "       bench/slurm/bench.sbatch build venv/bin/python",
    "venv/bin/python bench/plot.py --out figures bench/results/bench-<job>.json",
  ], 13);
  t.point(s, 0.6, 3.65, 3.85, 2.2, 1, "fsbench.py", "Metadata, 4 KiB files, 1–512 MiB I/O; 5 repeats, median.", { bodySize: 15 });
  t.point(s, 4.725, 3.65, 3.85, 2.2, 2, "workflow.py", "Three real DAGonStar workflows, 3 repeats; output size checked.", { bodySize: 15 });
  t.point(s, 8.85, 3.65, 3.85, 2.2, 3, "run.py", "--targets selects nfs, tmpfs, dagonfs-cs, dagonfs-p2p; --nfs-dir; BENCH_ARGS.", { badge: C.accent2, bodySize: 15 });
  s.addNotes("14:30–15:45. Use a Release build; debug logging and sanitizers change timing. The script stops at once if mpirun is not on PATH. Raw results go to bench/results/*.json; plot.py draws figures/bench-*.png.");

  // 15 — expected micro results
  s = content("Evaluate", "Expected: micro-benchmarks", 4);
  t.table(s, 0.6, 1.5, 7.6, [
    ["Median, one node", "NFS", "DAGonFS CS", "DAGonFS P2P", "tmpfs"],
    ["create (ops/s)", "67", "25,532", "26,943", "272,010"],
    ["stat (ops/s)", "151", "592,191", "42,318", "690,129"],
    ["4 KiB files written/s", "96", "5,923", "7,946", "119,477"],
    ["512 MiB write (MiB/s)", "283", "647", "841", "3,371"],
    ["512 MiB read (MiB/s)", "117", "1,186", "1,387", "5,521"],
  ], [2.6, 1.0, 1.3, 1.4, 1.3], { fontSize: 14, rowH: 0.5 });
  t.caption(s, 0.6, 4.65, 7.6, "Slurm job 19513: 2 × Xeon Gold 5218, Open MPI 4.1.4, libfuse 3.3.0, 4 ranks");
  t.stat(s, 8.7, 1.5, 4.0, "10²–10³×", "NFS on metadata: DAGonFS answers from the front end's memory");
  t.stat(s, 8.7, 3.8, 4.0, "⅕ – ⅓", "of tmpfs on large files: FUSE crossings and distribution copies", C.accent4);
  s.addText("Your numbers will differ with hardware and NFS load; the ratios are what to compare.", { x: 0.6, y: 5.2, w: 7.6, h: 0.9, fontSize: 15, color: C.text2, valign: "top", margin: 0, isTextBox: true });
  s.addNotes("15:45–17:00. Peer-to-peer stat is an order of magnitude below client-server because peer-to-peer disables the kernel attribute cache and replicates changes. NFS write grows with size because it writes back asynchronously until fsync.");

  // 16 — expected workflow results
  s = content("Evaluate", "Expected: DAGonStar workflows", 4);
  s.addChart(pres.charts.BAR, series({ cats: DATA.wfCats, ...DATA.wf }, NAMES, ["nfs", "cs", "p2p", "tmpfs"]), chartBase({
    x: 0.6, y: 1.4, w: 7.9, h: 5.3, barDir: "bar", chartColors: [SERIES.nfs, SERIES.cs, SERIES.p2p, SERIES.tmpfs],
    showValue: true, dataLabelPosition: "outEnd", dataLabelColor: H.dk2, dataLabelFormatCode: "0.0",
    valAxisMinVal: 0, valAxisMaxVal: 300, valAxisMajorUnit: 50, catAxisOrientation: "maxMin",
    title: "Wall time in seconds, median of 3, one node (lower is better)",
  }));
  t.card(s, 8.9, 1.5, 3.8, 2.4);
  s.addText([
    { text: "Speed-up over NFS", options: { bold: true, color: C.text1, breakLine: true } },
    { text: "1.9× · 2.0× · 10.4×", options: { bold: true, color: C.accent1, fontSize: 24, breakLine: true } },
    { text: "shell · Python · small files (client-server); 1.9–10.3× on two nodes", options: { color: C.text2 } },
  ], { x: 9.15, y: 1.65, w: 3.3, h: 2.15, fontSize: 14, valign: "top", margin: 0, isTextBox: true });
  t.card(s, 8.9, 4.15, 3.8, 2.5);
  s.addText([
    { text: "Reading your run", options: { bold: true, color: C.text1, breakLine: true } },
    { text: "Compare configurations within one run; differences under ~30 % between runs are noise. The DAGonStar floor is ≈ 7 s per workflow.", options: { color: C.text2 } },
  ], { x: 9.15, y: 4.3, w: 3.3, h: 2.25, fontSize: 14, valign: "top", margin: 0, isTextBox: true });
  s.addNotes("17:00–18:30. The large-file workflow writes and reads about 9 GiB; the Python reducer verifies every byte, so a fast but wrong run fails. Two runs four hours apart differed by −9 % to +31 % for DAGonFS; the NFS server's load is the largest uncertainty.");

  // 17 — checklist
  s = pres.addSlide({ masterName: "Closing (dark)", sectionTitle: "Evaluate" });
  s.addText("Checklist", { placeholder: "title" });
  const checks = [
    ["Compile", "ctest: 4/4 tests passed"],
    ["Install", "dagonfs and its lib/ staged on every node"],
    ["Configure", "model chosen; launcher --dry-run prints the expected mpirun line"],
    ["Test", "30/30 scenarios, replication and MPI-IO OK, workflow consistent, mounts gone"],
    ["Evaluate", "Release build, --mem set, medians compared within one run"],
  ];
  checks.forEach(([head, body], i) => {
    const y = 1.95 + i * 0.85;
    t.badge(s, 0.8, y, i + 1, i === 4 ? C.accent2 : C.accent1, 0.55, 16);
    s.addText(head, { x: 1.6, y, w: 2.2, h: 0.55, fontSize: 18, bold: true, color: C.background1, valign: "middle", margin: 0, isTextBox: true });
    s.addText(body, { x: 3.8, y, w: 8.7, h: 0.55, fontSize: 16, color: C.background2, valign: "middle", margin: 0, isTextBox: true });
  });
  s.addText("Docs: docs/CONFIGURATION.md · docs/TESTING.md · docs/PERFORMANCE.md · man docs/man/dagonfs.1", { x: 0.8, y: 6.4, w: 11.0, h: 0.4, fontSize: 13, color: C.accent2, margin: 0, isTextBox: true });
  s.addNotes("18:30–20:00. Recap and questions. Remind attendees to export results to persistent storage before unmounting: DAGonFS contents vanish with the job.");

  const file = path.join(OUT, "DAGonFS-tutorial.pptx");
  await pres.writeFile({ fileName: file });
  if (applyTheme) await applyTheme(file, THEME);
  return file;
}

(async () => {
  for (const f of [await scientific(), await tutorial()]) console.log("wrote", path.relative(ROOT, f));
})().catch((e) => { console.error(e); process.exit(1); });
