import datetime as dt
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from .db import Base


class Paper(Base):
    __tablename__ = "papers"
    id = Column(Integer, primary_key=True)
    ext_id = Column(String, unique=True, index=True)   # e.g. "arXiv:2506.14021"
    source = Column(String, index=True)                # arXiv / Google Scholar / IEEE / ACM / 学术新闻
    title = Column(Text)
    authors = Column(Text, default="[]")               # json list
    abstract = Column(Text, default="")
    url = Column(String, default="")
    venue = Column(String, default="")
    doi = Column(String, default="", index=True)
    topic = Column(String, index=True)                 # matched keyword
    published_at = Column(DateTime, index=True)
    fetched_at = Column(DateTime, default=dt.datetime.utcnow)
    # AI summary
    tldr = Column(Text, default="")
    method = Column(Text, default="")
    contributions = Column(Text, default="[]")         # json list


class SavedItem(Base):
    __tablename__ = "saved_items"
    id = Column(Integer, primary_key=True)
    paper_id = Column(Integer, index=True)
    saved = Column(Boolean, default=False)
    read = Column(Boolean, default=False)
    note = Column(Text, default="")
    feedback = Column(String, default="")   # "" | "up" | "down" (relevance learning)
    updated_at = Column(DateTime, default=dt.datetime.utcnow)


class UserSettings(Base):
    __tablename__ = "user_settings"
    id = Column(Integer, primary_key=True)             # always 1 (single user)
    keywords = Column(Text, default="[]")              # json list, max 3
    domain = Column(String, default="")
    sources = Column(Text, default="{}")               # json {name: bool}
    refresh_times = Column(String, default="10:00,22:00")
    backfill_n = Column(Integer, default=5)
    channels = Column(Text, default="{}")              # json {email:bool, browser:bool}
    email = Column(String, default="")


class TrendSnapshot(Base):
    __tablename__ = "trend_snapshots"
    id = Column(Integer, primary_key=True)
    domain = Column(String)
    window = Column(Integer, default=7)
    data = Column(Text, default="{}")                  # json {bars, top3}
    created_at = Column(DateTime, default=dt.datetime.utcnow)


class WeeklyReport(Base):
    __tablename__ = "weekly_reports"
    id = Column(Integer, primary_key=True)
    week_label = Column(String)
    data = Column(Text, default="{}")                  # json
    created_at = Column(DateTime, default=dt.datetime.utcnow)
