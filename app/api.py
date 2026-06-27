"""All HTTP routes. Returns shapes the frontend can render directly."""
import json
import asyncio
import datetime as dt
from zoneinfo import ZoneInfo
from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import Session

from .db import get_db, SessionLocal
from .config import settings
from .models import Paper, SavedItem, UserSettings, TrendSnapshot, WeeklyReport
from .pipeline.fetch import run_refresh, get_or_create_settings, is_refreshing, get_progress
from .pipeline.trends import compute_trends
from .pipeline.report import build_weekly_report
from .services.email import send_email


def _tz():
    try:
        return ZoneInfo(settings.TIMEZONE)
    except Exception:
        return dt.timezone.utc


def _now_local():
    return dt.datetime.now(_tz())


def _to_local(d: dt.datetime):
    """Treat stored naive datetimes as UTC, return them in the configured tz."""
    if not d:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(_tz())

router = APIRouter(prefix="/api")

SRC_STYLE = {
    "arXiv":           {"color": "#B31B1B", "bg": "#FBEAEA"},
    "Google Scholar":  {"color": "#1A73E8", "bg": "#E8F0FE"},
    "IEEE":            {"color": "#00629B", "bg": "#E1EEF6"},
    "ACM":             {"color": "#0F6FB5", "bg": "#E3F0F8"},
    "Crossref":        {"color": "#5B6470", "bg": "#EEF1F5"},
    "GitHub":          {"color": "#1F2328", "bg": "#EAECEF"},
    "学术新闻":         {"color": "#7C4DD9", "bg": "#F0EAFB"},
}
KW_COLORS = ["#2A5BD7", "#1F8A5B", "#7C4DD9"]


def _ago(d: dt.datetime):
    if not d:
        return ""
    delta = _now_local() - _to_local(d)
    h = delta.total_seconds() / 3600
    if h < 1:
        return "刚刚"
    if h < 24:
        return f"{int(h)} 小时前"
    days = int(h // 24)
    return "昨天" if days == 1 else f"{days} 天前"


def _is_today(d: dt.datetime):
    return bool(d) and _to_local(d).date() == _now_local().date()


def serialize_paper(p: Paper, saved_map, is_backfill=False):
    style = SRC_STYLE.get(p.source, SRC_STYLE["Crossref"])
    sv = saved_map.get(p.id)
    authors = json.loads(p.authors or "[]")
    return {
        "id": p.id, "source": p.source,
        "sourceColor": style["color"], "sourceBg": style["bg"],
        "topic": p.topic, "ago": _ago(p.published_at),
        "date": p.published_at.strftime("%Y-%m-%d") if p.published_at else "",
        "title": p.title, "venue": p.venue or "",
        "authors": ", ".join(authors[:4]) + (", et al." if len(authors) > 4 else ""),
        "abstract": p.abstract or "", "url": p.url,
        "tldr": p.tldr, "method": p.method,
        "contributions": json.loads(p.contributions or "[]"),
        "today": _is_today(p.published_at), "isBackfill": is_backfill,
        "saved": bool(sv and sv.saved), "read": bool(sv and sv.read),
        "note": (sv.note if sv else "") or "",
        "feedback": (sv.feedback if sv else "") or "",
    }


def _saved_map(db):
    return {s.paper_id: s for s in db.query(SavedItem).all()}


@router.get("/bootstrap")
def bootstrap(db: Session = Depends(get_db)):
    """Everything the dashboard needs in one shot."""
    s = get_or_create_settings(db)
    keywords = json.loads(s.keywords or "[]")
    enabled = json.loads(s.sources or "{}")
    channels = json.loads(s.channels or "{}")
    saved_map = _saved_map(db)

    # ---- feed: today's papers per keyword, backfill if a keyword has none today.
    #      GitHub repos (pushed_at is always "today") are kept SEPARATE and capped
    #      so they don't flood the feed or suppress the real-paper backfill.
    GH_PER_KW = 3
    feed_rows = []  # (Paper, is_backfill)
    today_count = 0
    has_backfill = False
    for kw in keywords:
        rows = (db.query(Paper).filter(Paper.topic == kw)
                .order_by(Paper.published_at.desc()).limit(50).all())
        papers = [p for p in rows if p.source != "GitHub"]
        gh = [p for p in rows if p.source == "GitHub"]
        today_rows = [p for p in papers if _is_today(p.published_at)]
        if today_rows:
            feed_rows += [(p, False) for p in today_rows]
            today_count += len(today_rows)
        else:
            back = papers[: s.backfill_n]
            feed_rows += [(p, True) for p in back]
            if back:
                has_backfill = True
        # a few GitHub repos as a supplementary real-time signal
        feed_rows += [(p, False) for p in gh[:GH_PER_KW]]
    # Order papers by REAL publication date (newest first); never by the "ago"
    # string, which would order "100 天前" before "14 天前". GitHub repos (whose
    # pushed_at is always "now") are then stably pushed to the BOTTOM so the
    # literature leads and repos stay a supplementary signal.
    feed_rows.sort(key=lambda pr: pr[0].published_at or dt.datetime.min, reverse=True)
    feed_rows.sort(key=lambda pr: pr[0].source == "GitHub")
    feed = [serialize_paper(p, saved_map, is_backfill=bf) for p, bf in feed_rows]

    # ---- library
    library = []
    for sv in db.query(SavedItem).filter(SavedItem.saved == True).all():  # noqa: E712
        p = db.get(Paper, sv.paper_id)
        if p:
            library.append(serialize_paper(p, saved_map))

    # ---- trends snapshot (latest cached)
    snap = (db.query(TrendSnapshot).order_by(TrendSnapshot.created_at.desc()).first())
    trends = json.loads(snap.data) if snap else {"bars": [], "top3": []}

    # ---- latest weekly report
    rep = db.query(WeeklyReport).order_by(WeeklyReport.created_at.desc()).first()
    report = json.loads(rep.data) if rep else None

    kw_objs = [{"no": i + 1, "term": kw, "color": KW_COLORS[i % 3],
                "matches": db.query(Paper).filter(Paper.topic == kw).count()}
               for i, kw in enumerate(keywords)]

    src_objs = [{"name": n, "color": SRC_STYLE.get(n, SRC_STYLE["Crossref"])["color"],
                 "bg": SRC_STYLE.get(n, SRC_STYLE["Crossref"])["bg"],
                 "on": enabled.get(n, True),
                 "desc": {"arXiv": "预印本", "Crossref": "期刊/会议元数据",
                          "Google Scholar": "综合学术检索", "GitHub": "实时仓库/论文代码",
                          "学术新闻": "RSS 动态"}.get(n, "")}
                for n in ["arXiv", "Crossref", "Google Scholar", "GitHub", "学术新闻"]]

    return {
        "configured": bool(keywords),
        "refreshing": is_refreshing(),
        "refreshStage": get_progress(),
        "feed": feed, "todayCount": today_count, "hasBackfill": has_backfill,
        "backfillN": s.backfill_n,
        "library": library,
        "trendBars": trends.get("bars", []), "top3": trends.get("top3", []),
        "report": report,
        "keywords": kw_objs, "sources": src_objs,
        "channels": channels, "domain": s.domain or "",
        "refreshTimes": s.refresh_times, "email": s.email or "",
    }


async def _refresh_job():
    """Background worker for a manual refresh. Does NOT email — the digest is
    sent only by the scheduler at the configured times (10:00 / 22:00)."""
    try:
        await run_refresh()
    except Exception as e:
        print(f"[refresh] job failed: {e}", flush=True)


@router.post("/refresh")
async def refresh():
    """Kick off a refresh in the background and return immediately.
    The smart pipeline (LLM query-expansion + relevance + summaries) can take a
    few minutes, so the UI polls /api/bootstrap (`refreshing` flag) for results."""
    if is_refreshing():
        return {"status": "already_running"}
    asyncio.create_task(_refresh_job())
    return {"status": "started"}


@router.post("/test-email")
def test_email(db: Session = Depends(get_db)):
    """Send a one-off test email to verify SMTP config without waiting for a refresh."""
    s = get_or_create_settings(db)
    to = s.email or ""
    html = ("<div style='font-family:sans-serif;max-width:560px'>"
            "<h2 style='color:#2A5BD7'>ScholarPulse · 测试邮件</h2>"
            "<p>如果你收到这封邮件，说明 SMTP 配置成功 ✅，自动文献摘要邮件已可正常发送。</p></div>")
    ok = send_email("ScholarPulse · 测试邮件", html, to)
    return {"sent": ok, "to": to or "(.env EMAIL_TO / SMTP_USER fallback)",
            "reason": "" if ok else "SMTP 未配置或发送失败，见后端日志 [email]"}


@router.get("/trends")
async def trends(domain: str = "", window: int = 7, db: Session = Depends(get_db)):
    s = get_or_create_settings(db)
    dom = domain or s.domain
    data = await compute_trends(dom, window)
    # only persist a non-empty result, so bootstrap never serves an empty snapshot
    if data.get("top3") or data.get("bars"):
        db.add(TrendSnapshot(domain=dom, window=window, data=json.dumps(data, ensure_ascii=False)))
        db.commit()
    return data


@router.get("/report/weekly")
async def weekly(db: Session = Depends(get_db)):
    return await build_weekly_report()


@router.post("/library/{paper_id}")
def update_library(paper_id: int, body: dict = Body(...), db: Session = Depends(get_db)):
    sv = db.query(SavedItem).filter(SavedItem.paper_id == paper_id).first()
    if not sv:
        sv = SavedItem(paper_id=paper_id)
        db.add(sv)
    for f in ("saved", "read"):
        if f in body:
            setattr(sv, f, bool(body[f]))
    if "note" in body:
        sv.note = str(body["note"])
    if "feedback" in body:
        fb = str(body["feedback"])
        sv.feedback = fb if fb in ("up", "down") else ""
    sv.updated_at = dt.datetime.utcnow()
    db.commit()
    return {"ok": True}


@router.get("/settings")
def get_settings(db: Session = Depends(get_db)):
    s = get_or_create_settings(db)
    return {
        "keywords": json.loads(s.keywords or "[]"),
        "domain": s.domain, "sources": json.loads(s.sources or "{}"),
        "refresh_times": s.refresh_times, "backfill_n": s.backfill_n,
        "channels": json.loads(s.channels or "{}"), "email": s.email,
    }


@router.put("/settings")
def put_settings(body: dict = Body(...), db: Session = Depends(get_db)):
    s = get_or_create_settings(db)
    if "keywords" in body:
        s.keywords = json.dumps(body["keywords"][:3], ensure_ascii=False)
    if "domain" in body:
        s.domain = body["domain"]
    if "sources" in body:
        s.sources = json.dumps(body["sources"], ensure_ascii=False)
    if "refresh_times" in body:
        s.refresh_times = body["refresh_times"]
    if "backfill_n" in body:
        s.backfill_n = int(body["backfill_n"])
    if "channels" in body:
        s.channels = json.dumps(body["channels"], ensure_ascii=False)
    if "email" in body:
        s.email = body["email"]
    db.commit()
    return {"ok": True}
