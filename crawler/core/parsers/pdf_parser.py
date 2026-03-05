# crawler/core/parsers/pdf_parser.py
from __future__ import annotations

import subprocess
import logging
import re
from typing import Any

from crawler.core.models import ParseResult, Segment

logger = logging.getLogger(__name__)

_RE_EXCESSIVE_SPACES = re.compile(r"[ \t]{3,}")


def _clean_pdf_text(text: str) -> str:
    cleaned = _RE_EXCESSIVE_SPACES.sub(" ", text)
    return cleaned.strip()


def parse_pdf(fetch_result: Any, url: str) -> ParseResult:
    if not fetch_result.body or len(fetch_result.body) < 100:
        logger.warning(f"PDF {url} ist leer oder zu klein zum Parsen.")
        return ParseResult(text="", segments=[], out_links=[])

    if b"%PDF-" not in fetch_result.body[:1024]:
        logger.warning(f"Datei von {url} ist kein valides PDF (Magic Bytes fehlen).")
        return ParseResult(text="", segments=[], out_links=[])

    try:
        proc = subprocess.run(
            ["pdftotext", "-layout", "-enc", "UTF-8", "-", "-"],
            input=fetch_result.body,
            capture_output=True,
            timeout=30,
        )

        stderr_output = proc.stderr.decode("utf-8", errors="ignore")

        if proc.returncode != 0 or "pdftotext version" in stderr_output.lower():
            logger.warning(
                f"pdftotext konnte PDF {url} nicht verarbeiten. Stderr: {stderr_output[:50]}..."
            )
            return ParseResult(text="", segments=[], out_links=[])

        full_text = proc.stdout.decode("utf-8", errors="replace")

        pages = full_text.split("\x0c")  # form-feed page break
        segments = []

        for i, page_text in enumerate(pages):
            cleaned_text = _clean_pdf_text(page_text)

            if cleaned_text:
                segments.append(
                    Segment(
                        order_index=i,
                        segment_type="pdf_page",
                        text=cleaned_text,
                        page_ref=str(i + 1),
                    )
                )

        return ParseResult(
            text=_clean_pdf_text(full_text),
            segments=segments,
            out_links=[],
        )

    except FileNotFoundError:
        logger.error(
            "'pdftotext' fehlt. Bitte Poppler installieren (z.B. 'brew install poppler')."
        )
        return ParseResult(text="", segments=[], out_links=[])
    except subprocess.TimeoutExpired:
        logger.warning(f"Timeout beim Parsen von PDF: {url}")
        return ParseResult(text="", segments=[], out_links=[])
    except Exception as e:
        logger.error(f"Unerwarteter Fehler bei PDF {url}: {e}")
        return ParseResult(text="", segments=[], out_links=[])