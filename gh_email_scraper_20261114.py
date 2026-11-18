#!/usr/bin/env python3
"""
gh_email_scraper_20261114.py

Public-data candidate contact collector (US software-development profiles).

Design notes
------------
* Only public, unauthenticated data is read: GitHub user search (10 req/min
  unauth), public profile HTML, public repository listing, public
  "commits by author" HTML and public `.patch` endpoints (which expose the
  author line of a commit).
* Every request is paced. Search API calls honour the 10/min quota and
  X-RateLimit-Reset; HTML calls are throttled per host.
* Emails are attributed to a profile only when GitHub itself attributes the
  commit to that account (`/commits?author=<login>`), so commits authored by
  other people in a shared repository are not attributed to the candidate.
* Resumable: raw findings are appended to emails.jsonl; already-processed
  logins are stored in processed.json; re-running continues where it stopped.
* `--exclude` accepts a blocklist file (one email or domain per line) so
  opt-outs are honoured on every run.

Subcommands
-----------
  run     discover candidates by US location + role keywords, then harvest
  enrich  harvest emails for logins already present in candidates.csv
  pages   extract emails from a list of public profile/repo URLs (any platform)
  stats   summarise what is in the output directory

Examples
--------
  python gh_email_scraper_20261114.py run --limit 120 --max-emails 100
  python gh_email_scraper_20261114.py run --limit 60 --out out_us_devs
  python gh_email_scraper_20261114.py pages --urls profiles.txt
  python gh_email_scraper_20261114.py stats --out out_us_devs

Requires Python 3.9+. Standard library only.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import sys
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.header import decode_header
from pathlib import Path
from urllib import error, request
from urllib.parse import quote

VERSION = "1.0"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
SEARCH_API = "https://api.github.com/search/users"

# --------------------------------------------------------------------------- #
# Search bank: US location variants x software-development role vocabulary
# --------------------------------------------------------------------------- #
DEFAULT_QUERIES = [
    'location:"United States" "software engineer" type:user',
    'location:"United States" developer type:user',
    'location:"United States" backend type:user',
    'location:"United States" fullstack type:user',
    'location:"United States" devops type:user',
    'location:"United States" "data engineer" type:user',
    'location:"United States" "machine learning" type:user',
    'location:"San Francisco" engineer type:user',
    'location:"New York" engineer type:user',
    'location:"Seattle" developer type:user',
    'location:"Austin" engineer type:user',
    'location:"Boston" developer type:user',
    'location:"Chicago" engineer type:user',
    'location:"Los Angeles" engineer type:user',
    'location:"Denver" developer type:user',
    'location:"Atlanta" engineer type:user',
    'location:"USA" developer type:user',
    'location:"Remote" "United States" engineer type:user',
]

# --------------------------------------------------------------------------- #
# Geography: positive US signals, negative non-US signals
# --------------------------------------------------------------------------- #
US_STATE_NAMES = [
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey",
    "new mexico", "new york", "north carolina", "north dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina",
    "south dakota", "tennessee", "texas", "utah", "vermont", "virginia",
    "west virginia", "wisconsin", "wyoming",
    "district of columbia",
]
US_STATE_ABBR = [
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id",
    "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms",
    "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
    "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv",
    "wi", "wy", "dc",
]
US_CITIES = [
    "san francisco", "bay area", "silicon valley", "new york city", "nyc",
    "brooklyn", "manhattan", "seattle", "bellevue", "redmond", "austin",
    "dallas", "houston", "san antonio", "boston", "chicago",
    "los angeles", "san diego", "san jose", "santa clara", "mountain view",
    "palo alto", "sunnyvale", "cupertino", "menlo park", "oakland", "berkeley",
    "denver", "boulder", "atlanta", "miami", "orlando", "tampa", "phoenix",
    "tempe", "scottsdale", "portland", "beaverton", "salt lake city",
    "raleigh", "durham", "charlotte", "minneapolis", "saint paul", "detroit",
    "ann arbor", "philadelphia", "pittsburgh", "baltimore", "washington dc",
    "nashville", "memphis", "columbus", "cleveland",
    "cincinnati", "indianapolis", "kansas city", "st louis", "saint louis",
    "sacramento", "las vegas", "albuquerque", "omaha", "madison", "milwaukee",
    "richmond", "virginia beach", "hartford", "providence", "buffalo",
    "rochester", "new haven", "boise", "tucson", "el paso", "fort worth",
    "plano", "irvine", "pasadena", "santa monica", "sf", "socal", "pdx",
    "atl", "dfw", "rdu", "slc",
]
NON_US = [
    "india", "bangalore", "bengaluru", "hyderabad", "pune", "mumbai", "delhi",
    "chennai", "noida", "gurgaon", "kolkata", "ahmedabad", "jaipur",
    "pakistan", "lahore", "karachi", "islamabad", "bangladesh", "dhaka",
    "sri lanka", "colombo", "nepal", "kathmandu",
    "nigeria", "lagos", "abuja", "kenya", "nairobi", "ghana", "accra",
    "ethiopia", "addis ababa", "egypt", "cairo", "morocco", "casablanca",
    "south africa", "johannesburg", "cape town", "tunisia", "algeria",
    "brazil", "brasil", "sao paulo", "são paulo", "rio de janeiro", "curitiba",
    "argentina", "buenos aires", "chile", "santiago", "colombia", "bogota",
    "bogotá", "medellin", "peru", "lima", "uruguay", "montevideo", "ecuador",
    "quito", "venezuela", "caracas", "mexico", "mexico city",
    "guadalajara", "monterrey", "costa rica", "panama", "guatemala",
    "canada", "toronto", "vancouver", "montreal", "ottawa", "calgary",
    "waterloo", "ontario", "quebec", "alberta", "british columbia",
    "uk", "united kingdom", "london", "manchester", "edinburgh", "glasgow",
    "bristol", "leeds", "england", "scotland", "wales",
    "ireland", "dublin", "cork", "germany", "berlin", "munich", "münchen",
    "hamburg", "frankfurt", "cologne", "stuttgart", "düsseldorf",
    "france", "paris", "lyon", "marseille", "toulouse", "nantes",
    "netherlands", "amsterdam", "rotterdam", "utrecht", "eindhoven",
    "belgium", "brussels", "antwerp", "spain", "madrid", "barcelona",
    "valencia", "seville", "portugal", "lisbon", "porto", "italy", "milan",
    "rome", "turin", "naples", "switzerland", "zurich", "zürich", "geneva",
    "lausanne", "austria", "vienna", "graz", "poland", "warsaw", "krakow",
    "kraków", "wroclaw", "wrocław", "gdansk", "poznan", "czech", "prague",
    "brno", "slovakia", "bratislava", "hungary", "budapest", "romania",
    "bucharest", "cluj", "timisoara", "bulgaria", "sofia", "serbia",
    "belgrade", "novi sad", "croatia", "zagreb", "slovenia", "ljubljana",
    "greece", "athens", "thessaloniki", "turkey", "istanbul", "ankara",
    "izmir", "russia", "moscow", "saint petersburg", "st petersburg",
    "ukraine", "kyiv", "kiev", "lviv", "kharkiv", "belarus", "minsk",
    "lithuania", "vilnius", "latvia", "riga", "estonia", "tallinn",
    "sweden", "stockholm", "gothenburg", "norway", "oslo", "denmark",
    "copenhagen", "finland", "helsinki", "iceland", "reykjavik",
    "israel", "tel aviv", "jerusalem", "haifa", "dubai", "uae",
    "united arab emirates", "abu dhabi", "saudi arabia", "riyadh", "qatar",
    "doha", "kuwait", "bahrain", "oman", "jordan", "amman", "lebanon",
    "beirut", "iraq", "iran", "tehran", "afghanistan", "kazakhstan", "almaty",
    "uzbekistan", "tashkent", "armenia", "yerevan", "tbilisi", "batumi",
    "azerbaijan", "baku", "china", "beijing", "shanghai", "shenzhen",
    "hangzhou", "guangzhou", "chengdu", "hong kong", "taiwan", "taipei",
    "japan", "tokyo", "osaka", "kyoto", "korea", "seoul", "busan",
    "singapore", "malaysia", "kuala lumpur", "indonesia", "jakarta",
    "bandung", "thailand", "bangkok", "vietnam", "hanoi", "ho chi minh",
    "philippines", "manila", "cebu", "myanmar", "cambodia", "laos",
    "australia", "sydney", "melbourne", "brisbane", "perth", "canberra",
    "new zealand", "auckland", "wellington", "christchurch",
]

US_STRONG = ["united states", "united states of america", "usa", "u.s.a",
             "u.s.", "us-based", "us based", "conus"]
US_WEAK_TOKENS = {"us", "u.s"}

PERSONAL_DOMAINS = {
    "gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "live.com",
    "msn.com", "yahoo.com", "ymail.com", "aol.com", "icloud.com", "me.com",
    "mac.com", "protonmail.com", "proton.me", "pm.me", "fastmail.com",
    "hey.com", "zoho.com", "gmx.com", "mail.com", "tutanota.com",
    "duck.com", "hushmail.com", "rocketmail.com",
}
DISPOSABLE_HINTS = {
    "mailinator.com", "tempmail.com", "guerrillamail.com", "10minutemail.com",
    "trashmail.com", "yopmail.com", "sharklasers.com", "getnada.com",
    "dispostable.com", "throwawaymail.com", "temp-mail.org", "fakeinbox.com",
}
ROLE_KEYWORDS = {
    "software engineer": 3, "software developer": 3, "swe": 2, "sde": 2,
    "engineer": 1, "developer": 1, "dev": 1, "programmer": 2,
    "backend": 2, "back-end": 2, "frontend": 2, "front-end": 2,
    "fullstack": 2, "full-stack": 2, "full stack": 2, "web developer": 2,
    "mobile": 1, "ios": 1, "android": 1, "devops": 3, "sre": 3,
    "platform engineer": 3, "infrastructure": 1, "cloud": 1, "kubernetes": 1,
    "data engineer": 3, "data scientist": 2, "machine learning": 2, "ml": 1,
    "ai engineer": 3, "architect": 1, "tech lead": 2, "engineering manager": 2,
    "cto": 2, "founder": 1, "typescript": 1, "javascript": 1, "python": 1,
    "golang": 1, "rust": 1, "java": 1, "react": 1, "node": 1, "nodejs": 1,
    "django": 1, "rails": 1, "ruby": 1, "php": 1, "laravel": 1, "c++": 1,
    "c#": 1, ".net": 1, "dotnet": 1, "scala": 1, "kotlin": 1, "swift": 1,
    "sql": 1, "postgres": 1, "aws": 1, "gcp": 1, "azure": 1, "terraform": 1,
    "microservices": 1, "api": 1, "security engineer": 2, "cybersecurity": 1,
}
RESERVED_GITHUB_PATHS = {
    "login", "join", "about", "pricing", "features", "topics", "collections",
    "trending", "sponsors", "settings", "notifications", "explore", "marketplace",
    "apps", "contact", "site", "search", "orgs", "users", "new", "organizations",
    "dashboard", "issues", "pulls", "codespaces", "copilot", "security", "readme",
    "customer-stories", "team", "enterprise", "events", "blog", "careers",
}
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
BAD_EMAIL_SUFFIX = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico",
                    ".css", ".js", ".json", ".woff", ".woff2", ".ttf")
BAD_EMAIL_DOMAINS = {"example.com", "example.org", "domain.com", "email.com",
                     "sentry.io", "sentry-next.wixpress.com", "localhost",
                     "github.com", "noreply.github.com", "w3.org"}
LOGIN_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9]|-(?=[A-Za-z0-9])){0,38}$")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def squash(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def strip_tags(html: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?</\1>", " ", html or "")
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = (text.replace("&amp;", "&").replace("&lt;", "<")
                .replace("&gt;", ">").replace("&quot;", '"')
                .replace("&#39;", "'").replace("&nbsp;", " "))
    return squash(text)


def decode_mime(value: str) -> str:
    try:
        parts = decode_header(value or "")
        out = []
        for chunk, enc in parts:
            if isinstance(chunk, bytes):
                out.append(chunk.decode(enc or "utf-8", "replace"))
            else:
                out.append(chunk)
        return squash("".join(out))
    except Exception:
        return squash(value or "")


def ascii_fold(text: str) -> str:
    return unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode()


def word_in(needle: str, haystack: str) -> bool:
    return re.search(r"(?<![a-z0-9])" + re.escape(needle) + r"(?![a-z0-9])",
                     haystack) is not None


def classify_us(location: str) -> tuple[str, str]:
    """Return (verdict, evidence). verdict in {us, maybe, non_us, unknown}.

    Order matters: an explicit US country token wins; a delimited US state
    abbreviation or full state name beats a foreign city name (e.g. "Dublin, CA"),
    while a foreign country token beats an ambiguous city ("Cambridge, UK").
    """
    loc = squash(ascii_fold(location).lower())
    if not loc:
        return "unknown", ""
    loc = re.sub(r"[|•·/;]", ",", loc)

    for hint in US_STRONG:
        if word_in(hint, loc):
            return "us", hint

    for abbr in US_STATE_ABBR:
        if re.search(r"(?:^|[,\s\(])" + abbr + r"(?:$|[,;\)\s])", loc):
            return "us", abbr.upper()
    for state in US_STATE_NAMES:
        if word_in(state, loc):
            return "us", state

    for hint in NON_US:
        if word_in(hint, loc):
            return "non_us", hint

    for city in US_CITIES:
        if word_in(city, loc):
            return "us", city

    for tok in US_WEAK_TOKENS:
        if word_in(tok, loc):
            return "maybe", tok
    return "maybe", loc[:60]


def classify_email(email: str) -> str:
    email = email.lower()
    domain = email.rsplit("@", 1)[-1]
    if domain.endswith("users.noreply.github.com") or domain == "noreply.github.com":
        return "github_noreply"
    if domain in PERSONAL_DOMAINS:
        return "personal"
    if re.search(r"\.edu(\.[a-z]{2})?$", domain):
        return "education"
    if domain in DISPOSABLE_HINTS or any(domain.endswith("." + d) for d in DISPOSABLE_HINTS):
        return "disposable"
    return "corporate"


def role_score(*chunks: str) -> tuple[int, str]:
    blob = " ".join(ascii_fold(c or "").lower() for c in chunks)
    score, hits = 0, []
    for kw, weight in ROLE_KEYWORDS.items():
        if word_in(kw, blob):
            score += weight
            hits.append(kw)
    return score, ";".join(sorted(set(hits))[:12])


def norm_email(value: str) -> str:
    return value.strip().strip(".,;:<>()[]\"'").lower()


def useful_email(email: str) -> bool:
    e = email.lower()
    if not re.fullmatch(r"[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}", e):
        return False
    if e.endswith(BAD_EMAIL_SUFFIX):
        return False
    domain = e.rsplit("@", 1)[-1]
    if domain in BAD_EMAIL_DOMAINS:
        return False
    if len(email) > 120 or ".." in e:
        return False
    if re.search(r"@[0-9.]+$", e):     # IP literals / version strings
        return False
    return True


class Pacer:
    """Serialises request start times across worker threads."""

    def __init__(self, interval: float, jitter: float = 0.35):
        self.interval = max(0.0, interval)
        self.jitter = jitter
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                if now >= self._next:
                    gap = self.interval * (1 + random.random() * self.jitter)
                    self._next = now + gap
                    return
                delay = self._next - now
            time.sleep(min(delay, 0.75))


class Http:
    def __init__(self, timeout: int = 30, retries: int = 2):
        self.timeout = timeout
        self.retries = retries
        self.count = 0
        self._count_lock = threading.Lock()

    def _bump(self) -> None:
        with self._count_lock:
            self.count += 1

    def get(self, url: str, pacer: "Pacer | None" = None,
            accept: str | None = None) -> tuple[int, str, dict]:
        headers = {
            "User-Agent": UA,
            "Accept": accept or "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        last_status, last_body, last_headers = 0, "", {}
        for attempt in range(self.retries + 1):
            if pacer:
                pacer.wait()
            self._bump()
            req = request.Request(url, headers=headers)
            try:
                with request.urlopen(req, timeout=self.timeout) as resp:
                    body = resp.read(4_000_000).decode("utf-8", "replace")
                    return resp.status, body, dict(resp.headers)
            except error.HTTPError as exc:
                body = ""
                try:
                    body = exc.read(400_000).decode("utf-8", "replace")
                except Exception:
                    pass
                last_status, last_body = exc.code, body
                last_headers = dict(exc.headers or {})
                if exc.code in (429, 403) and attempt < self.retries:
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    wait = float(retry_after) if retry_after and retry_after.isdigit() else 5.0 * (attempt + 1)
                    time.sleep(min(wait, 30))
                    continue
                if 500 <= exc.code < 600 and attempt < self.retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                return last_status, last_body, last_headers
            except Exception as exc:  # timeouts, dns, resets
                last_status = 0
                last_body = f"{type(exc).__name__}: {exc}"
                if attempt < self.retries:
                    time.sleep(1.2 * (attempt + 1))
                    continue
        return last_status, last_body, last_headers


# --------------------------------------------------------------------------- #
# GitHub scraping primitives
# --------------------------------------------------------------------------- #
def github_search_users(http: Http, pacer: Pacer, query: str,
                        per_page: int = 100, max_wait: int = 65,
                        max_pages: int = 3) -> list[str]:
    """Fetch up to max_pages of search results (GitHub caps at 1000 total)."""
    logins: list[str] = []
    seen: set[str] = set()
    for page in range(1, max_pages + 1):
        url = f"{SEARCH_API}?q={quote(query)}&per_page={per_page}&page={page}"
        status, body, headers = http.get(url, pacer=pacer,
                                         accept="application/vnd.github+json")
        if status == 403 and headers.get("X-RateLimit-Remaining") == "0":
            reset = int(headers.get("X-RateLimit-Reset") or 0)
            wait = max(2, min(max_wait, reset - int(time.time()) + 1))
            print(f"    search quota exhausted, sleeping {wait}s")
            time.sleep(wait)
            status, body, headers = http.get(url, pacer=pacer,
                                             accept="application/vnd.github+json")
        if status != 200:
            print(f"    search failed [{status}] page {page} for {query[:60]}")
            break
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            break
        items = data.get("items", [])
        if not items:
            break
        for item in items:
            login = item.get("login", "")
            if login and login.lower() not in seen:
                seen.add(login.lower())
                logins.append(login)
        # Last page (short) means no more results for this query
        if len(items) < per_page:
            break
    return logins


def parse_profile(html: str, login: str) -> dict:
    info = {
        "login": login,
        "profile_url": f"https://github.com/{login}",
        "name": "", "company": "", "location": "", "bio": "",
        "public_email": "", "website": "", "followers": "",
    }
    m = re.search(r'itemprop="name"[^>]*>\s*([^<]{1,120})', html)
    if m:
        info["name"] = squash(m.group(1))
    m = re.search(r'itemprop="worksFor".{0,3000}?<span class="p-label">(.*?)</span>', html, re.S)
    if not m:
        m = re.search(r'itemprop="worksFor".{0,3000}?<div>(.*?)</div>', html, re.S)
    if m:
        info["company"] = strip_tags(m.group(1))[:120]
    # GitHub renders the location as aria-label="Home location: <city>" plus a
    # <span class="p-label">; the icon SVG in between is long, so anchor on the
    # aria-label first and fall back to the label span.
    m = re.search(r'aria-label="Home location:\s*([^"]{1,120})"', html)
    if m:
        info["location"] = squash(m.group(1))[:120]
    if not info["location"]:
        m = re.search(r'itemprop="homeLocation".{0,3000}?<span class="p-label">(.*?)</span>', html, re.S)
        if m:
            info["location"] = strip_tags(m.group(1))[:120]
    m = re.search(r'data-bio-text="([^"]{0,600})"', html)
    if m:
        info["bio"] = squash(m.group(1))[:600]
    m = re.search(r'<li[^>]*itemprop="email"[^>]*>(.{0,400}?)</li>', html, re.S)
    if m:
        found = EMAIL_RE.search(m.group(1))
        if found:
            info["public_email"] = norm_email(found.group(0))
    if not info["public_email"]:
        m = re.search(r'href="mailto:([^"?]{3,120})"', html)
        if m:
            info["public_email"] = norm_email(m.group(1))
    m = re.search(r'itemprop="url".{0,800}?<a[^>]*href="(https?://[^"]{3,200})"', html, re.S)
    if m:
        info["website"] = m.group(1)[:200]
    m = re.search(r'<span class="text-bold color-fg-default">([\d,\.km]+)</span>\s*followers', html)
    if m:
        info["followers"] = m.group(1)
    return info


def fetch_profile(http: Http, pacer: Pacer, login: str) -> dict:
    status, body, _ = http.get(f"https://github.com/{login}", pacer=pacer)
    if status != 200:
        return {"login": login, "profile_url": f"https://github.com/{login}",
                "_error": f"profile[{status}]"}
    info = parse_profile(body, login)
    info["_html_len"] = len(body)
    return info


def fetch_repos(http: Http, pacer: Pacer, login: str, limit: int = 3) -> list[str]:
    status, body, _ = http.get(f"https://github.com/{login}?tab=repositories",
                               pacer=pacer)
    if status != 200:
        return []
    names, seen = [], set()
    # Markup: <a href="/owner/repo" itemprop="name codeRepository">repo</a>
    pairs = re.findall(
        r'<a[^>]*href="/([^/"]+)/([^/"?]+)"[^>]*itemprop="name codeRepository"', body)
    if pairs:
        candidates = [repo for owner, repo in pairs if owner.lower() == login.lower()]
    else:  # fallback: container-first ordering
        candidates = re.findall(r'itemprop="name codeRepository"[^>]*>\s*([^<\s]{1,120})</a>', body)
    for repo in candidates:
        if repo.lower() in seen or repo.lower() in RESERVED_GITHUB_PATHS:
            continue
        seen.add(repo.lower())
        names.append(repo)
        if len(names) >= limit:
            break
    return names


def fetch_author_commits(http: Http, pacer: Pacer, login: str, repo: str,
                         limit: int = 3) -> list[str]:
    url = f"https://github.com/{login}/{repo}/commits?author={login}"
    status, body, _ = http.get(url, pacer=pacer)
    if status != 200:
        return []
    shas, seen = [], set()
    pattern = rf"/{re.escape(login)}/{re.escape(repo)}/commit/([0-9a-f]{{40}})"
    for sha in re.findall(pattern, body):
        if sha in seen:
            continue
        seen.add(sha)
        shas.append(sha)
        if len(shas) >= limit:
            break
    return shas


def fetch_patch_author(http: Http, pacer: Pacer, login: str, repo: str,
                       sha: str) -> dict | None:
    url = f"https://github.com/{login}/{repo}/commit/{sha}.patch"
    status, body, _ = http.get(url, pacer=pacer, accept="text/plain")
    if status != 200 or not body:
        return None
    name, email_addr, date, subject = "", "", "", ""
    for line in body.splitlines()[:40]:
        if line.startswith("From: ") and not email_addr:
            m = re.match(r"^(.*?)\s*<([^>]+)>", line[6:])
            if m:
                name = decode_mime(m.group(1).strip().strip('"'))
                email_addr = norm_email(m.group(2))
        elif line.startswith("Date: ") and not date:
            date = line[6:].strip()
        elif line.startswith("Subject: ") and not subject:
            subject = line[9:].strip()
        elif line.startswith("diff --git"):
            break
    if not email_addr:
        return None
    return {"name": name, "email": email_addr, "date": date,
            "subject": decode_mime(subject), "sha": sha, "repo": repo}


# --------------------------------------------------------------------------- #
# Collection orchestration
# --------------------------------------------------------------------------- #
@dataclass
class Store:
    outdir: Path
    exclude_emails: set = field(default_factory=set)
    exclude_domains: set = field(default_factory=set)
    records: list = field(default_factory=list)
    seen_keys: set = field(default_factory=set)
    processed: set = field(default_factory=set)
    candidates: list = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self):
        self.outdir.mkdir(parents=True, exist_ok=True)
        self.emails_jsonl = self.outdir / "emails.jsonl"
        self.processed_json = self.outdir / "processed.json"
        self.candidates_csv = self.outdir / "candidates.csv"
        self.emails_csv = self.outdir / "emails.csv"
        if self.emails_jsonl.exists():
            for line in self.emails_jsonl.read_text("utf-8", "replace").splitlines():
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                self.records.append(rec)
                self.seen_keys.add((rec.get("email", ""), rec.get("login", "")))
        if self.processed_json.exists():
            try:
                self.processed = set(json.loads(self.processed_json.read_text("utf-8")))
            except Exception:
                self.processed = set()

    def blocked(self, email_addr: str) -> bool:
        domain = email_addr.rsplit("@", 1)[-1]
        return email_addr in self.exclude_emails or domain in self.exclude_domains

    def add(self, rec: dict) -> bool:
        key = (rec.get("email", ""), rec.get("login", ""))
        with self.lock:
            if key in self.seen_keys:
                return False
            self.seen_keys.add(key)
            self.records.append(rec)
            with self.emails_jsonl.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            return True

    def mark_processed(self, login: str) -> None:
        with self.lock:
            self.processed.add(login)
            self.processed_json.write_text(
                json.dumps(sorted(self.processed), ensure_ascii=False), "utf-8")

    def unique_emails(self) -> set:
        return {r.get("email", "") for r in self.records if r.get("email")}

    def write_csv(self) -> tuple[int, int]:
        email_fields = ["email", "name", "login", "company", "location",
                        "us_match", "us_evidence", "role_score", "role_signals",
                        "email_type", "email_source", "repo", "commit_sha",
                        "commit_date", "commit_subject", "profile_url",
                        "collected_at"]
        best: dict[str, dict] = {}
        rank = {"personal": 0, "corporate": 1, "education": 2, "github_noreply": 9}
        for rec in self.records:
            addr = rec.get("email", "")
            if not addr:
                continue
            cur = best.get(addr)
            if cur is None or rank.get(rec.get("email_type", ""), 5) < rank.get(cur.get("email_type", ""), 5):
                best[addr] = rec
        rows = sorted(best.values(), key=lambda r: (
            r.get("us_match") != "us", -int(r.get("role_score") or 0),
            r.get("email_type") != "personal", r.get("email", "")))
        with self.emails_csv.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=email_fields, extrasaction="ignore")
            writer.writeheader()
            for rec in rows:
                writer.writerow({k: rec.get(k, "") for k in email_fields})

        cand_fields = ["login", "name", "company", "location", "us_match",
                       "us_evidence", "role_score", "role_signals", "public_email",
                       "bio", "followers", "profile_url", "source_query", "status"]
        # merge with any candidate rows from earlier runs (later runs only know
        # about the profiles they just discovered)
        merged: dict[str, dict] = {}
        if self.candidates_csv.exists():
            with self.candidates_csv.open("r", encoding="utf-8-sig") as fh:
                for row in csv.DictReader(fh):
                    if row.get("login"):
                        merged[row["login"]] = dict(row)
        for rec in self.candidates:
            login = rec.get("login", "")
            if not login:
                continue
            base = merged.get(login, {})
            for k, v in rec.items():
                if v in ("", None):
                    continue
                if k == "status" and v == "new" and base.get("status"):
                    continue   # keep the status from the run that processed it
                base[k] = v
            merged[login] = base
        with self.candidates_csv.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=cand_fields, extrasaction="ignore")
            writer.writeheader()
            for rec in merged.values():
                writer.writerow({k: rec.get(k, "") for k in cand_fields})
        return len(rows), len(merged)

    def write_new_emails(self, before_emails: set, stamp: str | None = None) -> Path | None:
        """Write only emails first seen in this run to new_emails_YYYYMMDD_HHMMSS.csv."""
        email_fields = ["email", "name", "login", "company", "location",
                        "us_match", "us_evidence", "role_score", "role_signals",
                        "email_type", "email_source", "repo", "commit_sha",
                        "commit_date", "commit_subject", "profile_url",
                        "collected_at"]
        rank = {"personal": 0, "corporate": 1, "education": 2, "github_noreply": 9}
        best: dict[str, dict] = {}
        for rec in self.records:
            addr = rec.get("email", "")
            if not addr or addr in before_emails:
                continue
            cur = best.get(addr)
            if cur is None or rank.get(rec.get("email_type", ""), 5) < rank.get(cur.get("email_type", ""), 5):
                best[addr] = rec
        if not best:
            return None
        rows = sorted(best.values(), key=lambda r: (
            -int(r.get("role_score") or 0), r.get("email", "")))
        stamp = stamp or datetime.now().strftime("%Y%m%d_%H%M%S")
        path = self.outdir / f"new_emails_{stamp}.csv"
        with path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=email_fields, extrasaction="ignore")
            writer.writeheader()
            for rec in rows:
                writer.writerow({k: rec.get(k, "") for k in email_fields})
        return path


def process_candidate(http: Http, html_pacer: Pacer, store: Store,
                      cand: dict, args) -> list[dict]:
    login = cand["login"]
    info = fetch_profile(http, html_pacer, login)
    if info.get("_error"):
        cand["status"] = "profile_fetch_failed"
        return []

    location = info.get("location", "")
    verdict, evidence = classify_us(location)
    bio = info.get("bio", "")
    company = info.get("company", "")
    name = info.get("name", "")
    score, signals = role_score(bio, company, info.get("website", ""))

    cand.update({
        "name": name, "company": company, "location": location,
        "us_match": verdict, "us_evidence": evidence,
        "role_score": score, "role_signals": signals,
        "public_email": info.get("public_email", ""),
        "bio": bio[:280], "followers": info.get("followers", ""),
        "profile_url": info.get("profile_url", ""),
    })

    if verdict == "non_us":
        cand["status"] = "skipped_non_us"
        return []
    if verdict != "us" and not args.include_unknown_location:
        cand["status"] = f"skipped_location_{verdict}"
        return []
    if score < args.min_score and not args.no_role_filter:
        cand["status"] = f"skipped_role_score_{score}"
        return []

    results: list[dict] = []

    def build(addr, source, patch=None, repo=""):
        if not useful_email(addr) or store.blocked(addr):
            return None
        etype = classify_email(addr)
        if etype == "disposable":
            return None
        if etype == "github_noreply" and not args.include_noreply:
            return None
        return {
            "email": addr,
            "name": (patch or {}).get("name") or name or login,
            "login": login,
            "company": company,
            "location": location,
            "us_match": verdict,
            "us_evidence": evidence,
            "role_score": score,
            "role_signals": signals,
            "email_type": etype,
            "email_source": source,
            "repo": repo,
            "commit_sha": (patch or {}).get("sha", ""),
            "commit_date": (patch or {}).get("date", ""),
            "commit_subject": (patch or {}).get("subject", "")[:120],
            "profile_url": f"https://github.com/{login}",
            "collected_at": now_iso(),
        }

    if info.get("public_email"):
        rec = build(info["public_email"], "profile_field")
        if rec:
            results.append(rec)

    repos = fetch_repos(http, html_pacer, login, limit=args.repos_per_user)
    if not repos:
        cand["status"] = "no_public_repos"
        return results

    for repo in repos:
        shas = fetch_author_commits(http, html_pacer, login, repo,
                                    limit=args.commits_per_repo)
        if not shas:
            continue
        for sha in shas:
            patch = fetch_patch_author(http, html_pacer, login, repo, sha)
            if not patch:
                continue
            rec = build(patch["email"], "commit_author", patch, repo)
            if rec:
                results.append(rec)
        if results:
            break
    cand["status"] = "collected" if results else "no_email_found"
    return results


def discovery(args) -> list[dict]:
    http = Http()
    search_pacer = Pacer(interval=6.3)   # 10 req/min unauth, kept under quota
    queries = DEFAULT_QUERIES
    if args.queries_file:
        queries = [q.strip() for q in Path(args.queries_file).read_text("utf-8").splitlines()
                   if q.strip() and not q.strip().startswith("#")]
    if args.query:
        queries = list(args.query)

    found: dict[str, str] = {}
    for i, q in enumerate(queries, 1):
        if len(found) >= max(args.limit * 3, 300):
            break
        logins = github_search_users(http, search_pacer, q)
        new = 0
        for login in logins:
            if login.lower() not in found:
                found[login.lower()] = q
                new += 1
        print(f"  query {i}/{len(queries)}: +{new} (total {len(found)}) :: {q[:70]}")

    seeds = []
    if args.seed_logins:
        for line in Path(args.seed_logins).read_text("utf-8").splitlines():
            login = line.strip().lstrip("@")
            if LOGIN_RE.match(login):
                seeds.append({"login": login, "source_query": "seed"})

    candidates = [{"login": l, "source_query": q, "status": "new"}
                  for l, q in found.items()]
    candidates = seeds + candidates
    if args.shuffle:
        random.Random(args.seed).shuffle(candidates)
    return candidates[: args.limit]


def harvest(args, candidates: list[dict] | None = None) -> None:
    outdir = Path(args.out)
    store = Store(outdir)
    store.exclude_emails, store.exclude_domains = load_blocklist(args.exclude)

    if candidates:
        store.candidates = candidates
    else:
        if not store.candidates_csv.exists():
            sys.exit(f"no candidates.csv in {outdir}; run `run` first")
        with store.candidates_csv.open("r", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                if not row.get("login"):
                    continue
                rec = dict(row)          # keep name/company/location from prior runs
                rec.setdefault("source_query", "csv")
                if not rec.get("status"):
                    rec["status"] = "new"
                store.candidates.append(rec)
        if args.limit:
            store.candidates = store.candidates[: args.limit]

    done = {p.lower() for p in store.processed}
    pending = [c for c in store.candidates if c["login"].lower() not in done]
    print(f"candidates: {len(store.candidates)} | already processed: "
          f"{len(store.candidates) - len(pending)} | pending: {len(pending)}")
    before_emails = store.unique_emails()
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    http = Http()
    html_pacer = Pacer(interval=args.pace, jitter=0.5)
    started = time.time()
    stop = threading.Event()

    def work(cand: dict) -> tuple[dict, list[dict]]:
        try:
            return cand, process_candidate(http, html_pacer, store, cand, args)
        except Exception as exc:
            cand["status"] = f"error:{type(exc).__name__}"
            return cand, []

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(work, c) for c in pending]
        for idx, fut in enumerate(futures, 1):
            cand, recs = fut.result()
            for rec in recs:
                store.add(rec)
            store.mark_processed(cand["login"])
            if recs:
                for rec in recs:
                    print(f"  [{idx}/{len(futures)}] {rec['email']:<42} "
                          f"{rec['us_match']:<6} role={rec['role_score']:<3} "
                          f"{rec['email_type']}")
            if args.max_emails and len(store.unique_emails()) >= args.max_emails:
                print(f"reached --max-emails {args.max_emails}; stopping")
                stop.set()
                for f in futures:
                    f.cancel()
                break
            if idx % 10 == 0:
                store.write_csv()

    emails, cands = store.write_csv()
    new_path = store.write_new_emails(before_emails, stamp=run_stamp)
    new_count = 0
    if new_path and new_path.exists():
        with new_path.open("r", encoding="utf-8-sig") as fh:
            new_count = sum(1 for _ in csv.DictReader(fh))
        print(f"new emails this run: {new_count} -> {new_path.resolve()}")
    else:
        print("new emails this run: 0")
    summary = {
        "generated_at": now_iso(),
        "out_dir": str(outdir.resolve()),
        "candidates_evaluated": len([c for c in store.candidates if c.get("status") != "new"]),
        "unique_emails": emails,
        "new_emails": new_count,
        "new_emails_file": str(new_path.resolve()) if new_path else "",
        "requests": http.count,
        "elapsed_seconds": round(time.time() - started, 1),
        "filters": {
            "min_role_score": args.min_score,
            "include_unknown_location": args.include_unknown_location,
            "include_noreply": args.include_noreply,
            "repos_per_user": args.repos_per_user,
            "commits_per_repo": args.commits_per_repo,
        },
        "status_counts": counts(store.candidates),
        "email_type_counts": counts(store.records, "email_type"),
        "us_match_counts": counts(store.records, "us_match"),
    }
    (outdir / "summary.json").write_text(json.dumps(summary, indent=2), "utf-8")
    print(json.dumps(summary, indent=2))


def counts(rows: list[dict], key: str = "status") -> dict:
    out: dict[str, int] = {}
    for row in rows:
        out[str(row.get(key, "?"))] = out.get(str(row.get(key, "?")), 0) + 1
    return dict(sorted(out.items(), key=lambda kv: -kv[1]))


def load_blocklist(path: str | None) -> tuple[set, set]:
    emails, domains = set(), set()
    if not path:
        return emails, domains
    for line in Path(path).read_text("utf-8", "replace").splitlines():
        item = line.strip().lower()
        if not item or item.startswith("#"):
            continue
        if item.startswith("@"):
            domains.add(item[1:])
        elif "@" in item:
            emails.add(item)
        else:
            domains.add(item)
    return emails, domains


# --------------------------------------------------------------------------- #
# "Other platforms": generic public page extraction
# --------------------------------------------------------------------------- #
def pages_mode(args) -> None:
    urls = []
    if args.urls:
        urls = [u.strip() for u in Path(args.urls).read_text("utf-8").splitlines()
                if u.strip() and not u.strip().startswith("#")]
    urls += list(args.url or [])
    if not urls:
        sys.exit("pages: pass --urls file.txt or --url https://...")

    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)
    http = Http()
    pacer = Pacer(interval=args.pace, jitter=0.5)
    exclude_emails, exclude_domains = load_blocklist(args.exclude)
    rows = []

    for url in urls:
        status, body, _ = http.get(url, pacer=pacer)
        if status != 200:
            print(f"  [{status}] {url}")
            continue
        title = ""
        m = re.search(r"(?is)<title[^>]*>(.*?)</title>", body)
        if m:
            title = squash(strip_tags(m.group(1)))[:160]
        text = strip_tags(body)
        found: dict[str, int] = {}
        for raw in EMAIL_RE.findall(body) + EMAIL_RE.findall(text):
            addr = norm_email(raw)
            if not useful_email(addr) or addr in exclude_emails:
                continue
            if addr.rsplit("@", 1)[-1] in exclude_domains:
                continue
            found[addr] = found.get(addr, 0) + 1
        for addr, hits in found.items():
            pos = text.lower().find(addr.lower())
            context = text[max(0, pos - 90): pos + 160] if pos >= 0 else ""
            score, signals = role_score(title, context)
            rows.append({
                "email": addr,
                "email_type": classify_email(addr),
                "page_title": title,
                "role_score": score,
                "role_signals": signals,
                "mentions": hits,
                "context": context,
                "source_url": url,
                "collected_at": now_iso(),
            })
        print(f"  [200] {url} -> {len(found)} email(s)")

    rows.sort(key=lambda r: (-int(r["mentions"]), r["email"]))
    out_csv = outdir / "page_emails.csv"
    fields = ["email", "email_type", "page_title", "role_score", "role_signals",
              "mentions", "context", "source_url", "collected_at"]
    with out_csv.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows -> {out_csv}")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Collect publicly disclosed candidate emails for US software-development profiles.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    p.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    sub = p.add_subparsers(dest="command", required=True)

    def common(sp):
        sp.add_argument("--out", default="out_us_devs", help="output directory")
        sp.add_argument("--limit", type=int, default=0, help="max candidates processed")
        sp.add_argument("--max-emails", type=int, default=0, help="stop after N unique emails")
        sp.add_argument("--concurrency", type=int, default=3, help="parallel workers (default 3)")
        sp.add_argument("--pace", type=float, default=0.55, help="seconds between HTML requests")
        sp.add_argument("--repos-per-user", type=int, default=2)
        sp.add_argument("--commits-per-repo", type=int, default=3)
        sp.add_argument("--min-score", type=int, default=2, help="role relevance threshold")
        sp.add_argument("--no-role-filter", action="store_true")
        sp.add_argument("--include-unknown-location", action="store_true",
                        help="also keep profiles whose location is not recognisably US")
        sp.add_argument("--include-noreply", action="store_true",
                        help="include @users.noreply.github.com addresses (do not receive mail)")
        sp.add_argument("--exclude", help="blocklist file: one email or domain per line")

    run = sub.add_parser("run", help="discover US candidates, then harvest emails")
    common(run)
    run.add_argument("--query", action="append", help="custom search query (repeatable)")
    run.add_argument("--queries-file", help="file with one search query per line")
    run.add_argument("--seed-logins", help="file with extra GitHub logins")
    run.add_argument("--no-shuffle", dest="shuffle", action="store_false", default=True)
    run.add_argument("--seed", type=int, default=20261114)

    en = sub.add_parser("enrich", help="harvest emails for logins already in candidates.csv")
    common(en)

    pg = sub.add_parser("pages", help="extract emails from public profile/repo URLs")
    pg.add_argument("--out", default="out_pages")
    pg.add_argument("--urls", help="file with one URL per line")
    pg.add_argument("--url", action="append", help="URL (repeatable)")
    pg.add_argument("--pace", type=float, default=0.8)
    pg.add_argument("--exclude")

    st = sub.add_parser("stats", help="summarise an output directory")
    st.add_argument("--out", default="out_us_devs")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "run":
        args.limit = args.limit or 100
        cands = discovery(args)
        print(f"discovered {len(cands)} candidate logins")
        harvest(args, cands)
    elif args.command == "enrich":
        harvest(args, None)
    elif args.command == "pages":
        pages_mode(args)
    elif args.command == "stats":
        outdir = Path(args.out)
        em = outdir / "emails.csv"
        if not em.exists():
            sys.exit(f"no emails.csv in {outdir}")
        with em.open("r", encoding="utf-8-sig") as fh:
            rows = list(csv.DictReader(fh))
        print(json.dumps({
            "unique_emails": len(rows),
            "by_type": counts(rows, "email_type"),
            "by_us_match": counts(rows, "us_match"),
            "top_domains": dict(sorted(
                {d: sum(1 for r in rows if r["email"].endswith("@" + d))
                 for d in {r["email"].rsplit("@", 1)[-1] for r in rows}}.items(),
                key=lambda kv: -kv[1])[:15]),
        }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
