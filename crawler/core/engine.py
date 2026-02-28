# crawler/core/engine.py
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Iterable, Mapping, Optional, Set, Tuple
from urllib.parse import urlsplit

import requests

from crawler.core.canonical import Canonicalizer
from crawler.core.models import CrawlTask, FetchResult
from crawler.core.parsers.html_parser import parse_html
from crawler.core.parsers.pdf_parser import parse_pdf  
from crawler.core.scheduler import PriorityScheduler
from crawler.core.storage import Storage, default_worker_id

try:
    import crawler.core.traps as traps  # type: ignore
except Exception:
    traps = None  # type: ignore


def _is_trap(url: str) -> bool:
    if traps is None:
        return False
    for fn_name in ("is_trap_url", "is_trap", "is_trap_link"):
        fn = getattr(traps, fn_name, None)
        if callable(fn):
            try:
                return bool(fn(url))
            except Exception:
                return False
    return False


@dataclass(frozen=True)
class EngineLimits:
    max_depth: int = 12  # Erhöht für tiefe RIS-Strukturen
    max_pages_per_muni: int = 25000  # Erhöht für Vollständigkeit in großen Städten
    max_file_size_mb: int = 100  # Erhöht für große Haushaltspläne/PDFs


class Engine:
    def __init__(
        self,
        keywords: dict,
        limits: EngineLimits = EngineLimits(),
        user_agent: str = "KIT-ClimatePolicyCrawler/1.0 (research)",
        request_timeout_seconds: int = 20,
        max_redirects: int = 10,
        respect_robots_txt: bool = False,
        min_delay_seconds_per_domain: float = 0.2,
        canonicalizer: Optional[Canonicalizer] = None,
        allowed_domains_by_muni: Optional[Mapping[str, Set[str]]] = None,
        worker_id: Optional[str] = None,
    ) -> None:
        self.storage = Storage()
        self.scheduler = PriorityScheduler()
        self.keywords = keywords
        self.limits = limits

        self.worker_id = worker_id or default_worker_id()

        self.timeout_connect = 5.0
        self.timeout_read = float(request_timeout_seconds)
        self.max_redirects = int(max_redirects)
        self.respect_robots_txt = bool(respect_robots_txt)
        self.min_delay_seconds_per_domain = float(min_delay_seconds_per_domain)

        self.canon = canonicalizer or Canonicalizer(
            strip_fragment=True,
            drop_query_prefixes=["utm_", "pk_"],
            drop_query_keys=["fbclid", "gclid", "session", "jsessionid", "sid", "phpsessid", "msclkid"],
            normalize_trailing_slash=True,
        )

        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "text/html,application/pdf,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
            }
        )
        self._session.max_redirects = self.max_redirects

        self._last_request_ts_by_domain: Dict[str, float] = {}
        self._pages_by_muni: Dict[str, int] = {}

        self.allowed_domains_by_muni: Dict[str, Set[str]] = {
            str(k): {self._norm_domain(d) for d in v if d}
            for k, v in (allowed_domains_by_muni or {}).items()
        }

    @staticmethod
    def _norm_domain(domain: str) -> str:
        d = (domain or "").strip().lower()
        if not d:
            return ""
        return d.split(":", 1)[0].rstrip(".")

    @staticmethod
    def _split(url: str):
        try:
            return urlsplit(url)
        except Exception:
            return None

    def _scheme_domain(self, url: str) -> Tuple[str, str]:
        s = self._split(url)
        if s is None:
            return "", ""
        return (s.scheme or "").lower(), self._norm_domain(s.netloc)

    def _is_allowed(self, muni_id: str, url: str) -> bool:
        scheme, domain = self._scheme_domain(url)
        if scheme not in ("http", "https") or not domain:
            return False
        allowed = self.allowed_domains_by_muni.get(muni_id)
        if not allowed:
            return False
        return domain in allowed 

    def score(self, url: str, anchor: Optional[str]) -> int:
        """
        High-Recall Scoring Engine: 
        Priorisiert PDFs, Ratsinformationssysteme und Klimabegriffe massiv.
        """
        u = (url or "").lower()
        a = (anchor or "").lower()
        score = 10  # Basis-Score für Entdeckung

        # 1. Höchste Priorität: PDFs (Goldquelle für Paper)
        if ".pdf" in u:
            score += 300

        # 2. Hohe Priorität: Ratsinformationssysteme (RIS)
        # Typische Pfade für SessionNet, Allris, etc.
        ris_patterns = ['session', 'bi/vo', 'bi/si', 'bi/kp', 'allris', 'ratsinfo', 'ris.']
        if any(p in u for p in ris_patterns):
            score += 200

        # 3. Inhaltliche Keywords (Klima, Finanzen, Bau)
        high_impact_keywords = [
            'klima', 'energie', 'wärme', 'solar', 'pv', 'wind', 'strom', 
            'förder', 'zuschuss', 'mittel', 'haushalt', 'finanz', 'euro',
            'bau', 'planung', 'sanierung', 'mobilität', 'verkehr',
            'rat', 'beschluss', 'sitzung', 'nki', 'kfw', 'umwelt'
        ]
        
        for kw in high_impact_keywords:
            if kw in u:
                score += 150
            if a and kw in a:
                score += 100

        # 4. Standard Positive Keywords aus Konfiguration
        for kw in self.keywords.get("positive", []):
            k = str(kw).lower()
            if k and k in u: score += 10
            if k and a and k in a: score += 5

        # 5. Negative Keywords (Noise reduzieren, aber Pfad nicht blockieren)
        for kw in self.keywords.get("negative", []):
            k = str(kw).lower()
            if k and k in u: score -= 50
            if k and a and k in a: score -= 20

        return score

    def _polite_sleep(self, domain: str) -> None:
        if not domain or self.min_delay_seconds_per_domain <= 0:
            return
        now = time.time()
        last = self._last_request_ts_by_domain.get(domain)
        if last is not None:
            dt = now - last
            if dt < self.min_delay_seconds_per_domain:
                time.sleep(self.min_delay_seconds_per_domain - dt)
        self._last_request_ts_by_domain[domain] = time.time()

    def fetch(self, url: str) -> FetchResult:
        _, domain = self._scheme_domain(url)
        self._polite_sleep(domain)

        try:
            resp = self._session.get(
                url,
                timeout=(self.timeout_connect, self.timeout_read),
                allow_redirects=True,
                headers={"Connection": "close"},
            )
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout) as e:
            raise RuntimeError(f"timeout:{type(e).__name__}") from e
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"request_error:{type(e).__name__}:{e}") from e

        headers = {str(k): str(v) for k, v in (resp.headers or {}).items()}
        return FetchResult(
            url_final=str(resp.url),
            status_code=int(resp.status_code),
            content_type=str(resp.headers.get("Content-Type") or "") or None,
            body=resp.content or b"",
            headers=headers,
        )

    def _over_size_limit(self, fr: FetchResult) -> bool:
        lim_bytes = int(self.limits.max_file_size_mb) * 1024 * 1024
        cl = fr.headers.get("Content-Length")
        if cl:
            try:
                return int(cl) > lim_bytes
            except ValueError:
                return False
        return False

    @staticmethod
    def _looks_like_html(url: str, content_type: Optional[str]) -> bool:
        ctype = (content_type or "").lower()
        if "text/html" in ctype or "application/xhtml" in ctype:
            return True
        u = (url or "").lower()
        return u.endswith((".html", ".htm"))

    @staticmethod
    def _looks_like_pdf(url: str, content_type: Optional[str]) -> bool:
        ctype = (content_type or "").lower()
        if "application/pdf" in ctype:
            return True
        u = (url or "").lower()
        return u.endswith(".pdf")

    def run_claimed_batch(self, batch_size: int = 1) -> None:
        seeds = self.storage.claim_next_seed_jobs(worker_id=self.worker_id, limit=batch_size)
        if not seeds:
            print("[engine] no pending seed_jobs", flush=True)
            return
        self.run(seeds)

    def run(self, seeds: Iterable[Tuple[str, str]]) -> None:
        canon = self.canon.normalize
        max_depth = int(self.limits.max_depth)
        max_pages = int(self.limits.max_pages_per_muni)
        lim_bytes = int(self.limits.max_file_size_mb) * 1024 * 1024

        for muni_id, url in seeds:
            muni_id = str(muni_id)
            self._pages_by_muni.setdefault(muni_id, 0)

            if muni_id not in self.allowed_domains_by_muni:
                _, d = self._scheme_domain(url)
                if d:
                    self.allowed_domains_by_muni[muni_id] = {d}

            url_c_seed = canon(url) or url
            if not self._is_allowed(muni_id, url):
                self.storage.mark_visited(url_c_seed, -1, "seed out of scope")
                continue

            self.scheduler.enqueue(CrawlTask(muni_id, url, depth=0), 100)

        while self.scheduler.has_next():
            task = self.scheduler.next()

            if task.depth > max_depth:
                continue
            if not self._is_allowed(task.municipality_id, task.url):
                continue
            if _is_trap(task.url):
                continue

            pages = self._pages_by_muni.get(task.municipality_id, 0)
            if pages >= max_pages:
                continue

            url_c = canon(task.url)
            if not url_c:
                continue

            if self.storage.is_visited(url_c):
                doc_id = self.storage.get_document_id_by_canonical_url(url_c)
                if doc_id is not None:
                    self.storage.link_document_to_municipality(task.municipality_id, doc_id)
                continue

            try:
                t0 = time.time()
                print(f"[fetch] depth={task.depth} muni={task.municipality_id} url={task.url}", flush=True)

                fr = self.fetch(task.url)
                status = int(fr.status_code)

                if not self._is_allowed(task.municipality_id, fr.url_final):
                    self.storage.mark_visited(url_c, status, "redirect out of scope")
                    continue

                if self._over_size_limit(fr):
                    self.storage.mark_visited(url_c, status, "oversize")
                    continue

                if not fr.body or len(fr.body) > lim_bytes:
                    self.storage.mark_visited(url_c, status, "body error/limit")
                    continue

                with self.storage.transaction():
                    doc_id = self.storage.store_raw(task.municipality_id, url_c, fr)

                    if self._looks_like_html(task.url, fr.content_type):
                        parse_result = parse_html(fr, fr.url_final)
                        
                        # High-Recall: Wir speichern Text, wenn er nicht komplett leer ist.
                        # Filterung erfolgt später im LLM-Schritt.
                        if len(parse_result.segments) > 0:
                            self.storage.store_segments(doc_id, parse_result.segments)

                        next_depth = task.depth + 1
                        if next_depth <= max_depth:
                            for link, anchor in parse_result.out_links:
                                link_c = canon(link)
                                if not link_c or not self._is_allowed(task.municipality_id, link_c):
                                    continue
                                
                                prio = self.score(link_c, anchor)
                                self.scheduler.enqueue(
                                    CrawlTask(
                                        task.municipality_id,
                                        link_c,
                                        depth=next_depth,
                                        parent_url=url_c,
                                        anchor_text=anchor,
                                    ),
                                    prio,
                                )

                    elif self._looks_like_pdf(task.url, fr.content_type):
                        print(f"[parse] Extrahiere PDF: {fr.url_final}", flush=True)
                        parse_result = parse_pdf(fr, fr.url_final)
                        self.storage.store_segments(doc_id, parse_result.segments)

                    self.storage.mark_visited(url_c, status)

                self._pages_by_muni[task.municipality_id] = pages + 1

            except Exception as e:
                self.storage.mark_visited(url_c, -1, str(e))

        for muni_id, _ in seeds:
            try:
                self.storage.finish_seed_job(str(muni_id), ok=True)
            except Exception:
                pass