import os

MD_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "SYSTEM_ARCHITECTURE_GUIDE.md"))
HTML_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "SYSTEM_ARCHITECTURE_GUIDE.html"))

def convert_md_to_printable_html():
    if not os.path.exists(MD_PATH):
        print(f"Error: {MD_PATH} not found.")
        return

    with open(MD_PATH, "r", encoding="utf-8") as f:
        md_content = f.read()

    # Basic markdown parsing for printable HTML format
    import html
    
    html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Distributed RAG System — Technical Architecture & File Guide</title>
    <style>
        @media print {{
            body {{ font-size: 11pt; }}
            .no-print {{ display: none; }}
            pre, code {{ page-break-inside: avoid; }}
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            line-height: 1.6;
            color: #1a202c;
            max-width: 900px;
            margin: 0 auto;
            padding: 40px 20px;
        }}
        h1, h2, h3, h4 {{ color: #0f172a; margin-top: 1.5em; font-family: inherit; }}
        h1 {{ border-bottom: 2px solid #0284c7; padding-bottom: 10px; font-size: 24pt; }}
        h2 {{ border-bottom: 1px solid #e2e8f0; padding-bottom: 6px; font-size: 16pt; }}
        code {{ background: #f1f5f9; padding: 2px 6px; border-radius: 4px; font-family: monospace; font-size: 0.9em; }}
        pre {{ background: #0f172a; color: #f8fafc; padding: 16px; border-radius: 8px; overflow-x: auto; font-family: monospace; }}
        pre code {{ background: transparent; color: inherit; padding: 0; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #cbd5e1; padding: 10px; text-align: left; }}
        th {{ background: #f8fafc; font-weight: 600; }}
        blockquote {{ border-left: 4px solid #0284c7; margin: 0; padding-left: 16px; color: #475569; }}
        .print-btn {{
            background: #0284c7; color: white; border: none; padding: 10px 20px; border-radius: 6px; font-size: 14px; font-weight: 600; cursor: pointer; margin-bottom: 20px;
        }}
    </style>
</head>
<body>
    <button class="print-btn no-print" onclick="window.print()">🖨️ Print / Save as PDF</button>
    <div id="content">
        <pre style="background: transparent; color: inherit; white-space: pre-wrap; font-family: inherit;">{html.escape(md_content)}</pre>
    </div>
</body>
</html>"""

    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html_template)

    print(f"Created printable HTML guide at: {HTML_PATH}")

if __name__ == "__main__":
    convert_md_to_printable_html()

