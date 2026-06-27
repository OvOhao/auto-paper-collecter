"""Main refresh pipeline: fetch → dedup → summarize → store."""
import json
import asyncio
import datetime as dt

from sqlalchemy.exc import IntegrityError

from ..db import SessionLocal
from ..models import Paper, UserSettings, SavedItem
from ..sources import arxiv, crossref, semanticscholar, rss_news, github
from .dedup import dedup
from .summarize import summarize
from .smart import expand_queries, filter_relevant

# Only one refresh may run at a time. The dashboard's "立即刷新" button (and the
# scheduler) can otherwise fire overlapping runs that race on papers.ext_id.
_refresh_lock = asyncio.Lock()

# Human-readable progress of the running refresh, surfaced to the UI via bootstrap.
_progress = ""


def _set_progress(msg):
    global _progress
    _progress = msg
    print(f"[refresh] {msg}", flush=True)


def get_progress():
    return _progress

SOURCE_FUNCS = {
    "arXiv": arxiv.search,
    "Crossref": crossref.search,         # also yields IEEE/ACM-attributed items
    "Google Scholar": semanticscholar.search,
    "GitHub": github.search,             # real-time repos / paper code / awesome-lists
    "学术新闻": rss_news.search,
}


def get_or_create_settings(db):
    s = db.get(UserSettings, 1)
    if not s:
        s = UserSettings(
            id=1, keywords="[]", domain="", sources=json.dumps({
                "arXiv": True, "Crossref": True, "Google Scholar": True,
                "GitHub": True, "学术新闻": True
            }), refresh_times="10:00,22:00", backfill_n=5,
            channels=json.dumps({"email": False, "browser": True}), email="",
        )
        db.add(s); db.commit(); db.refresh(s)
    return s


async def _gather_for_keyword(kw, enabled, domain="", negatives=None):
    # 1) the LLM expands the keyword into effective, associative search strings.
    #    Capped at 3 — more multiplies requests to rate-limited APIs (arXiv 429).
    queries = (await expand_queries(kw, domain))[:3]
    print(f"[fetch] '{kw}' -> queries {queries}", flush=True)

    # 2) fetch every (source, query) combo concurrently, with a politeness cap
    #    so rate-limited APIs (arXiv) aren't hammered. The semaphore is created
    #    per-call to stay bound to the current event loop (API vs scheduler).
    sem = asyncio.Semaphore(5)

    async def one(fn, q):
        async with sem:
            try:
                return await fn(q)
            except Exception as e:
                print(f"[fetch] source error for {q!r}: {e}", flush=True)
                return []

    tasks = [one(fn, q)
             for name, fn in SOURCE_FUNCS.items() if enabled.get(name, True)
             for q in queries]
    results = await asyncio.gather(*tasks)
    items = []
    for res in results:
        items.extend(res)

    # 3) unique within this keyword before the (costly) relevance pass
    uniq = {}
    for it in items:
        if it.ext_id and it.ext_id not in uniq:
            uniq[it.ext_id] = it
    items = list(uniq.values())

    # 4) the LLM drops off-domain / off-topic papers (and learns from 👎 negatives)
    before = len(items)
    items = await filter_relevant(items, kw, domain, negatives=negatives)
    print(f"[fetch] '{kw}' relevance filter: {before} -> {len(items)}", flush=True)

    for it in items:
        it.topic = kw  # tag matched keyword
    return items


async def run_refresh(max_summaries: int = 40):
    """Fetch all keywords from all enabled sources, summarize NEW papers, store."""
    if _refresh_lock.locked():
        print("[refresh] another refresh is already running; skipping")
        return {"new": 0, "keywords": 0, "busy": True}

    async with _refresh_lock:
        db = SessionLocal()
        try:
            s = get_or_create_settings(db)
            keywords = json.loads(s.keywords or "[]")
            enabled = json.loads(s.sources or "{}")
            domain = s.domain or ""
            if not keywords:
                print("[refresh] no keywords set; skipping")
                return {"new": 0, "keywords": 0}

            # titles the user marked 👎 — fed to the relevance filter as negatives
            disliked = [row[0] for row in db.query(Paper.title)
                        .join(SavedItem, SavedItem.paper_id == Paper.id)
                        .filter(SavedItem.feedback == "down").limit(30).all()]

            kws = keywords[:3]
            raw = []
            for i, kw in enumerate(kws, 1):
                _set_progress(f"抓取与过滤：{kw}（{i}/{len(kws)}）")
                raw.extend(await _gather_for_keyword(kw, enabled, domain, negatives=disliked))

            # Collapse near-duplicates across sources (by DOI / title), then make the
            # batch unique on ext_id too — that is the DB's UNIQUE column, and the
            # DOI/title dedup key does not always coincide with it.
            raw = dedup(raw)
            by_ext = {}
            for it in raw:
                if it.ext_id and it.ext_id not in by_ext:
                    by_ext[it.ext_id] = it
            raw = list(by_ext.values())

            existing = {row[0] for row in db.query(Paper.ext_id).all()}

            # Drop Crossref's garbage future-dated records (e.g. year 2121) — they
            # would otherwise sort to the very top of the recency-ordered feed.
            horizon = dt.datetime.utcnow() + dt.timedelta(days=2)
            new_items = [it for it in raw
                         if it.ext_id and it.ext_id not in existing
                         and (not it.published_at or it.published_at <= horizon)]
            new_items.sort(key=lambda x: x.published_at or dt.datetime.min, reverse=True)

            # Summarize the first `max_summaries` NEW papers concurrently (the LLM),
            # capped so we don't flood the gateway. This is the slowest step, so
            # running it in parallel is the big win.
            _EMPTY = {"tldr": "", "method": "", "contributions": []}
            to_summ = new_items[:max_summaries]
            _set_progress(f"生成中文摘要（{len(to_summ)} 篇）…")
            sem = asyncio.Semaphore(5)

            async def _summ(it):
                async with sem:
                    return await summarize(it)

            summaries = await asyncio.gather(*[_summ(it) for it in to_summ])
            _set_progress("写入数据库…")
            paired = list(zip(to_summ, summaries)) + [(it, _EMPTY) for it in new_items[max_summaries:]]

            count = 0
            for it, summ in paired:
                p = Paper(
                    ext_id=it.ext_id, source=it.source, title=it.title,
                    authors=json.dumps(it.authors, ensure_ascii=False),
                    abstract=it.abstract, url=it.url, venue=it.venue, doi=it.doi or "",
                    topic=getattr(it, "topic", ""),
                    # keep undated papers as NULL (don't fake "刚刚" — that would
                    # float dateless Crossref records to the top of the feed)
                    published_at=it.published_at,
                    fetched_at=dt.datetime.utcnow(),
                    tldr=summ["tldr"], method=summ["method"],
                    contributions=json.dumps(summ["contributions"], ensure_ascii=False),
                )
                db.add(p)
                # Commit per paper so one duplicate ext_id (e.g. inserted by a
                # concurrent run) skips that row instead of failing the whole batch.
                try:
                    db.commit()
                except IntegrityError:
                    db.rollback()
                    continue
                count += 1
            print(f"[refresh] {count} new papers across {len(keywords)} keywords", flush=True)
            return {"new": count, "keywords": len(keywords)}
        finally:
            _set_progress("")
            db.close()


def is_refreshing() -> bool:
    """True while a refresh is in progress (for the UI to poll)."""
    return _refresh_lock.locked()
