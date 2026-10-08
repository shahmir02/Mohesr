"""
Standalone CLI script to generate the Data Documentation PDF.
Outputs: docs/DATA_DOCUMENTATION.pdf
"""

import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.pdf_generator import generate_pdf_documentation


def main():
    pdf_path = os.path.join(PROJECT_ROOT, "docs", "DATA_DOCUMENTATION.pdf")
    print(f"[+] Generating Data Documentation PDF at: {pdf_path}")
    res = generate_pdf_documentation(pdf_path)
    print(f"[✓] Data Documentation PDF created successfully ({os.path.getsize(res)} bytes).")


if __name__ == "__main__":
    main()
