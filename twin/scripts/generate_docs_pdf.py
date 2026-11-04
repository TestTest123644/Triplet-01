#!/usr/bin/env python3
"""
PDF Documentation Generator for TSYP 14 City Brain Convergence (Twin Layer).
Converts Markdown guides into polished, presentation-grade PDF documents with
embedded screenshots, executive color palette, styled tables, and running headers/footers.
"""

import os
import re
import sys
import markdown
import xhtml2pdf.pisa as pisa

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DOCS_DIR = os.path.join(BASE_DIR, "docs")
IMAGES_DIR = os.path.join(DOCS_DIR, "images")

# Global Executive Technical Styling
PDF_CSS = """
@page {
    size: a4 portrait;
    margin: 1.8cm 1.5cm 1.8cm 1.5cm;
    @top-left {
        content: "TSYP 14 City Brain Convergence | Twin Layer (Layer 1)";
        font-family: Helvetica, Arial, sans-serif;
        font-size: 7.5pt;
        font-weight: bold;
        color: #475569;
    }
    @top-right {
        content: "Technical Architecture & Evaluation Manual";
        font-family: Helvetica, Arial, sans-serif;
        font-size: 7.5pt;
        color: #64748b;
    }
    @bottom-left {
        content: "CONFIDENTIAL & PROPRIETARY — HACKATHON JURY EVALUATION";
        font-family: Helvetica, Arial, sans-serif;
        font-size: 7pt;
        color: #94a3b8;
    }
    @bottom-right {
        content: "Page " counter(page) " of " counter(pages);
        font-family: Helvetica, Arial, sans-serif;
        font-size: 7.5pt;
        font-weight: bold;
        color: #475569;
    }
}

body {
    font-family: Helvetica, Arial, sans-serif;
    font-size: 9pt;
    line-height: 1.5;
    color: #1e293b;
}

h1 {
    font-size: 19pt;
    font-weight: bold;
    color: #0f172a;
    border-bottom: 2.5px solid #0284c7;
    padding-bottom: 6px;
    margin-top: 0;
    margin-bottom: 12px;
}

h2 {
    font-size: 13.5pt;
    font-weight: bold;
    color: #0f172a;
    border-bottom: 1px solid #cbd5e1;
    padding-bottom: 4px;
    margin-top: 18px;
    margin-bottom: 8px;
}

h3 {
    font-size: 11pt;
    font-weight: bold;
    color: #0369a1;
    margin-top: 12px;
    margin-bottom: 6px;
}

h4 {
    font-size: 10pt;
    font-weight: bold;
    color: #334155;
    margin-top: 10px;
    margin-bottom: 4px;
}

p {
    margin-top: 4px;
    margin-bottom: 8px;
}

ul, ol {
    margin-top: 4px;
    margin-bottom: 8px;
    padding-left: 20px;
}

li {
    margin-bottom: 3px;
}

/* Code Blocks */
pre, pre.codeblock {
    background-color: #0f172a;
    color: #f8fafc;
    font-family: Courier, monospace;
    font-size: 7.8pt;
    line-height: 1.45;
    padding: 8px 12px;
    border-radius: 4px;
    border: 1px solid #1e293b;
    margin: 8px 0;
}

/* Inline Code only */
code {
    font-family: Courier, monospace;
    font-size: 8.2pt;
    background-color: #f1f5f9;
    color: #0369a1;
    padding: 1px 4px;
    border-radius: 3px;
}

/* Tables */
table {
    width: 100%;
    border-collapse: collapse;
    margin: 10px 0;
    font-size: 8.5pt;
}

th {
    background-color: #1e293b;
    color: #ffffff;
    font-weight: bold;
    text-align: left;
    padding: 6px 8px;
    border: 1px solid #334155;
}

td {
    padding: 5px 8px;
    border: 1px solid #e2e8f0;
    vertical-align: top;
}

td code {
    font-size: 7.2pt;
    padding: 0 2px;
}

tr:nth-child(even) td {
    background-color: #f8fafc;
}

/* Blockquotes / Formula Callout Panels */
blockquote {
    border-left: 3.5px solid #0284c7;
    background-color: #f8fafc;
    padding: 8px 12px;
    margin: 8px 0;
    color: #0f172a;
    font-size: 8.8pt;
    font-family: Courier, monospace;
}

blockquote p {
    margin: 0;
    padding: 0;
}

/* Images */
.figure-container {
    text-align: center;
    margin: 12px 0;
}

img {
    max-width: 520px;
    border: 1px solid #cbd5e1;
    border-radius: 4px;
}

.caption {
    font-size: 8pt;
    color: #64748b;
    font-style: italic;
    margin-top: 4px;
    text-align: center;
}

hr {
    border: none;
    border-top: 1px solid #e2e8f0;
    margin: 14px 0;
}
"""

def sanitize_text_for_pdf(text: str) -> str:
    # Replace unicode box-drawing and math glyphs with clean ASCII equivalents for Courier
    glyph_map = {
        '┌': '+', '┐': '+', '└': '+', '┘': '+',
        '├': '+', '┤': '+', '┬': '+', '┴': '+', '┼': '+',
        '─': '-', '│': '|', '═': '=', '║': '|',
        '▼': 'v', '▲': '^', '▶': '>', '◀': '<',
        '•': '*', '≈': '~', '≠': '!=', '≤': '<=', '≥': '>=',
        'Δ': 'd', 'λ': 'lambda', 'σ': 'sigma', 'α': 'alpha', 'β': 'beta', 'γ': 'gamma'
    }
    for k, v in glyph_map.items():
        text = text.replace(k, v)
    return text

def markdown_to_html(md_content: str, title: str) -> str:
    # 1. Clean box drawing characters for PDF Courier font compatibility
    cleaned_md = sanitize_text_for_pdf(md_content)

    # 2. Rewrite relative image links to absolute filesystem paths
    def replace_img(match):
        alt = match.group(1)
        rel_path = match.group(2)
        filename = os.path.basename(rel_path)
        abs_img = os.path.join(IMAGES_DIR, filename)
        return f'<div class="figure-container"><img src="{abs_img}" alt="{alt}" /><div class="caption">{alt}</div></div>'

    processed_md = re.sub(r'!\[([^\]]*)\]\(([^)]+)\)', replace_img, cleaned_md)

    # 3. Convert markdown to HTML with standard extensions
    html_body = markdown.markdown(
        processed_md,
        extensions=['tables', 'fenced_code', 'def_list', 'nl2br']
    )

    # 4. Strip <code> tags from inside <pre> so xhtml2pdf never renders white highlight boxes on code lines
    html_body = re.sub(r'<pre><code[^>]*>', '<pre class="codeblock">', html_body)
    html_body = html_body.replace('</code></pre>', '</pre>')

    full_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{title}</title>
<style>
{PDF_CSS}
</style>
</head>
<body>
{html_body}
</body>
</html>
"""
    return full_html


def build_pdf(md_filename: str, pdf_filename: str, title: str):
    md_path = os.path.join(DOCS_DIR, md_filename)
    pdf_path = os.path.join(DOCS_DIR, pdf_filename)

    if not os.path.exists(md_path):
        print(f"Error: {md_path} not found.")
        sys.exit(1)

    with open(md_path, 'r', encoding='utf-8') as f:
        md_text = f.read()

    html_content = markdown_to_html(md_text, title)

    with open(pdf_path, 'wb') as pdf_file:
        pisa_status = pisa.CreatePDF(html_content, dest=pdf_file)

    if pisa_status.err:
        print(f"Error generating {pdf_filename}: error code {pisa_status.err}")
        sys.exit(1)
    else:
        file_size_kb = os.path.getsize(pdf_path) / 1024
        print(f"Successfully generated: {pdf_path} ({file_size_kb:.1f} KB)")


def main():
    os.makedirs(DOCS_DIR, exist_ok=True)
    print("Building presentation-grade PDF documents...")

    docs_to_generate = [
        ("DEVELOPER_GUIDE.md", "DEVELOPER_GUIDE.pdf", "Twin Layer - Developer & Integration Guide"),
        ("USER_GUIDE.md", "USER_GUIDE.pdf", "Twin Layer - Operations & User Guide"),
        ("PHYSICS_AND_ADVERSARY_SPEC.md", "PHYSICS_AND_ADVERSARY_SPEC.pdf", "Twin Layer - Physics & Adversary Specifications")
    ]

    for md_file, pdf_file, doc_title in docs_to_generate:
        build_pdf(md_file, pdf_file, doc_title)

    print("\nAll PDF documents successfully compiled.")


if __name__ == "__main__":
    main()
