"""Ubah laporan Markdown menjadi PDF (A4, Liberation Serif 12 pt, spasi 1) lewat Chromium headless.

Pemakaian: python build_pdf.py <laporan.md> <output.pdf>
HTML sementara ditulis di samping berkas .md (Chromium snap tidak bisa membaca /tmp), lalu dihapus.
"""
import re
import subprocess
import sys
from pathlib import Path

import markdown

CSS = """
@page {
  size: A4;
  margin: 2.5cm;
  @bottom-center { content: counter(page); font-family: 'Liberation Serif'; font-size: 11pt; }
  @top-right { content: "IT Platform and Infrastructure – Laporan Tugas 2 Kelompok 21";
               font-family: 'Liberation Serif'; font-size: 9pt; font-style: italic; color: #444; }
}
@page :first { @bottom-center { content: none; } @top-right { content: none; } }
body { font-family: 'Liberation Serif', 'Times New Roman', serif; font-size: 12pt;
       line-height: 1.15; text-align: justify; color: #000; }
h1, h2, h3 { text-align: left; break-after: avoid-page; }
h2 + p, h3 + p { break-before: avoid-page; }
h2 { font-size: 14pt; margin: 12pt 0 5pt; }
h3 { font-size: 12pt; margin: 9pt 0 3pt; }
p { margin: 0 0 6pt; }
ul, ol { margin: 0 0 6pt; padding-left: 20pt; }
li { margin-bottom: 1pt; }
table { border-collapse: collapse; width: 100%; margin: 4pt 0 8pt; font-size: 10pt;
        line-height: 1.1; text-align: left; }
thead { display: table-header-group; }
tr { page-break-inside: avoid; break-inside: avoid; }
th, td { border: 0.6pt solid #000; padding: 2pt 4pt; vertical-align: top; }
th { background: #e8e8e8; }
pre { font-family: 'Liberation Mono', monospace; font-size: 7.5pt; line-height: 1.15;
      border: 0.6pt solid #999; padding: 4pt 6pt; white-space: pre; overflow: hidden;
      page-break-inside: avoid; break-inside: avoid; text-align: left; }
code { font-family: 'Liberation Mono', monospace; font-size: 10pt; }
pre code { font-size: 7.5pt; }
img { width: 100%; display: block; margin: 4pt 0 2pt; }
p:has(> img) { page-break-inside: avoid; break-inside: avoid; }
.cover { text-align: center; page-break-after: always; break-after: page; padding-top: 3cm; }
.cover h1 { font-size: 20pt; margin-bottom: 4pt; text-align: center; }
.cover h2 { font-size: 15pt; margin: 0 0 36pt; text-align: center; }
.cover p { margin: 0 0 10pt; text-align: center; }
.cover table { width: 75%; margin: 18pt auto 36pt; font-size: 12pt; }
.cover img { width: 4cm; margin: 0 auto 24pt; }
h2.pustaka { break-before: page; margin-top: 0; }
img[src$='rencana-spark.svg'] { width: 85%; margin: 4pt auto 2pt; }
"""


def add_blank_line_before_lists(text):
    # Python-Markdown hanya mengenali daftar bila diawali baris kosong.
    lines = text.split("\n")
    out = []
    for line in lines:
        is_item = re.match(r"^(\d+\.|[-*]) ", line)
        prev = out[-1] if out else ""
        prev_is_item_or_cont = re.match(r"^(\d+\.|[-*]) |^\s+\S", prev)
        if is_item and prev.strip() and not prev_is_item_or_cont:
            out.append("")
        out.append(line)
    return "\n".join(out)


def main():
    md_path, pdf_path = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()
    text = md_path.read_text(encoding="utf-8")
    cover_md, body_md = text.split("\n---\n", 1)
    body_md = re.sub(r"(?m)^---\s*$", "", body_md)

    extensions = ["tables", "fenced_code"]
    cover_html = markdown.markdown(cover_md, extensions=extensions + ["nl2br"])
    body_html = markdown.markdown(add_blank_line_before_lists(body_md), extensions=extensions)
    # Daftar Pustaka selalu dimulai di halaman baru.
    body_html = body_html.replace("<h2>Daftar Pustaka</h2>", "<h2 class='pustaka'>Daftar Pustaka</h2>")

    html = (f"<!doctype html><html lang='id'><head><meta charset='utf-8'>"
            f"<title>Laporan Tugas 2 Kelompok 21</title><style>{CSS}</style></head>"
            f"<body><div class='cover'>{cover_html}</div>{body_html}</body></html>")
    tmp_html = md_path.with_name("_laporan_tmp.html")
    tmp_html.write_text(html, encoding="utf-8")
    try:
        subprocess.run(
            ["chromium", "--headless", "--disable-gpu", "--no-pdf-header-footer",
             f"--print-to-pdf={pdf_path}", tmp_html.as_uri()],
            check=True, capture_output=True, timeout=120,
        )
    finally:
        tmp_html.unlink(missing_ok=True)
    print(f"PDF ditulis ke {pdf_path}")


if __name__ == "__main__":
    main()
