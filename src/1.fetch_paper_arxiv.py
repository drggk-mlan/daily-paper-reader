import arxiv
import json
import os
import time
from datetime import datetime, timedelta, timezone

# 项目根目录（当前脚本位于 src/ 下）
SCRIPT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
CONFIG_FILE = os.path.join(ROOT_DIR, "config.yaml")
CRAWL_STATE_FILE = os.path.join(ROOT_DIR, "archive", "crawl_state.json")
SEEN_IDS_FILE = os.path.join(ROOT_DIR, "archive", "arxiv_seen.json")

# ArXiv 的主要一级分类列表
# 注意：物理学比较特殊，ArXiv 历史上有很多独立的物理存档，为了保险，我们列出主要的
CATEGORIES_TO_FETCH = [
    "cs", "math", "stat", "q-bio", "q-fin", "eess", "econ",
    "physics", "cond-mat", "hep-ph", "hep-th", "gr-qc", "astro-ph",
]


def load_config() -> dict:
    if not os.path.exists(CONFIG_FILE):
        return {}
    try:
        import yaml  # type: ignore
    except Exception:
        log("[WARN] 未安装 PyYAML，无法解析 config.yaml。")
        return {}
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data if isinstance(data, dict) else {}
    except Exception as e:
        log(f"[WARN] 读取 config.yaml 失败：{e}")
        return {}


def resolve_days_window(default_days: int) -> int:
    config = load_config()
    paper_setting = (config or {}).get("arxiv_paper_setting") or {}
    crawler_setting = (config or {}).get("crawler") or {}

    value = paper_setting.get("days_window")
    if value is None:
        value = crawler_setting.get("days_window")
    try:
        days = int(value)
        return max(days, 1)
    except Exception:
        return max(default_days, 1)


def load_last_crawl_at() -> datetime | None:
    if not os.path.exists(CRAWL_STATE_FILE):
        return None
    try:
        with open(CRAWL_STATE_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f) or {}
    except Exception:
        return None
    raw = str(payload.get("last_crawl_at") or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def save_last_crawl_at(at_time: datetime) -> None:
    os.makedirs(os.path.dirname(CRAWL_STATE_FILE), exist_ok=True)
    payload = {"last_crawl_at": at_time.astimezone(timezone.utc).isoformat()}
    with open(CRAWL_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_seen_state() -> tuple[set[str], datetime | None]:
    if not os.path.exists(SEEN_IDS_FILE):
        return set(), None
    try:
        with open(SEEN_IDS_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f) or {}
    except Exception:
        return set(), None

    raw_ids = payload.get("ids") or []
    if not isinstance(raw_ids, list):
        raw_ids = []
    seen_ids = {str(i).strip() for i in raw_ids if str(i).strip()}

    raw_latest = str(payload.get("latest_published_at") or "").strip()
    latest_dt = None
    if raw_latest:
        try:
            latest_dt = datetime.fromisoformat(raw_latest.replace("Z", "+00:00"))
            if latest_dt.tzinfo is None:
                latest_dt = latest_dt.replace(tzinfo=timezone.utc)
            latest_dt = latest_dt.astimezone(timezone.utc)
        except Exception:
            latest_dt = None

    return seen_ids, latest_dt


def save_seen_state(seen_ids: set[str], latest_published_at: datetime | None) -> None:
    os.makedirs(os.path.dirname(SEEN_IDS_FILE), exist_ok=True)
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "latest_published_at": latest_published_at.astimezone(timezone.utc).isoformat()
        if latest_published_at
        else "",
        "ids": sorted(seen_ids),
    }
    with open(SEEN_IDS_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def log(message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def group_start(title: str) -> None:
    print(f"::group::{title}", flush=True)


def group_end() -> None:
    print("::endgroup::", flush=True)


def fetch_all_domains_metadata_robust(
    days: int | None = None,
    output_file: str | None = None,
) -> None:
    # 1. 计算时间窗口（优先使用上次抓取时间）
    end_date = datetime.now(timezone.utc)
    seen_ids, latest_published_at = load_seen_state()
    if days is None:
        days = resolve_days_window(1)
    if latest_published_at:
        start_date = latest_published_at
        source_desc = "latest_published_at"
    else:
        last_crawl_at = load_last_crawl_at()
        if last_crawl_at:
            start_date = last_crawl_at
            source_desc = "last_crawl_at"
        else:
            start_date = end_date - timedelta(days=days)
            source_desc = f"days_window={days}"

    # 兜底：无论来源如何，都不早于 (now - days_window)
    start_date = max(start_date, end_date - timedelta(days=days))

    if start_date >= end_date:
        start_date = end_date - timedelta(minutes=1)

    start_str = start_date.strftime("%Y%m%d%H%M")
    end_str = end_date.strftime("%Y%m%d%H%M")
    
    group_start("Step 1 - fetch arXiv")
    log(f"🌍 [Global Ingest] Window: {start_str} TO {end_str} ({source_desc})")
    
    # 结果集使用字典去重 (因为有些论文跨领域，比如同时在 cs 和 stat)
    unique_papers = {}
    max_published_new: datetime | None = None
    
    client = arxiv.Client(
        page_size=200,    # 降级：从 1000 降到 200，避免单次响应过大导致 500
        delay_seconds=3.0,
        num_retries=5
    )

    # 2. 遍历分类进行抓取
    for category in CATEGORIES_TO_FETCH:
        group_start(f"Fetch category: {category}")
        log(f"🚀 Fetching category: {category} ...")
        
        # 构造查询：cat:cs* AND submittedDate[...]
        # 使用通配符 category* 以覆盖子领域 (如 cs.AI, cs.LG)
        query = f"cat:{category}* AND submittedDate:[{start_str} TO {end_str}]"
        
        search = arxiv.Search(
            query=query,
            max_results=None,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending
        )
        
        count = 0
        try:
            for r in client.results(search):
                pid = r.get_short_id()

                if pid in seen_ids:
                    continue
                
                # 如果这篇论文已经存在（被其他分类抓过了），跳过
                if pid in unique_papers:
                    continue
                    
                # 使用 PDF 链接而不是摘要页链接，方便后续直接下载或传给下游处理
                pdf_link = getattr(r, "pdf_url", None) or r.entry_id
                paper_dict = {
                    "id": pid,
                    "source": "arxiv",
                    "title": r.title.replace("\n", " "),
                    "abstract": r.summary.replace("\n", " "),
                    "authors": [a.name for a in r.authors],
                    "primary_category": r.primary_category,
                    "categories": r.categories,
                    "published": str(r.published),
                    "link": pdf_link,
                }
                unique_papers[pid] = paper_dict
                count += 1

                seen_ids.add(pid)
                published_dt = r.published
                if isinstance(published_dt, datetime):
                    if published_dt.tzinfo is None:
                        published_dt = published_dt.replace(tzinfo=timezone.utc)
                    published_dt = published_dt.astimezone(timezone.utc)
                    if max_published_new is None or published_dt > max_published_new:
                        max_published_new = published_dt
                
                if count % 100 == 0:
                    log(f"   Category {category}: {count} papers fetched...")
            
            log(f"   ✅ Finished {category}: Got {count} new papers.")
            
        except Exception as e:
            # 单个分类失败不影响大局，打印错误继续下一个
            log(f"   ❌ Error fetching category {category}: {e}")
            time.sleep(5) # 出错后多歇一会
        finally:
            group_end()

    # 3. 保存汇总结果
    total_count = len(unique_papers)
    log(f"✅ All Done. Total unique papers fetched: {total_count}")
    
    if total_count > 0:
        # 若未显式指定输出文件，则按日期命名到项目根目录下的 archive/YYYYMMDD/raw 目录：
        # <ROOT_DIR>/archive/YYYYMMDD/raw/arxiv_papers_YYYYMMDD.json
        if not output_file:
            today_str = end_date.strftime("%Y%m%d")
            archive_dir = os.path.join(ROOT_DIR, "archive", today_str)
            raw_dir = os.path.join(archive_dir, "raw")
            output_file = os.path.join(
                raw_dir,
                f"arxiv_papers_{today_str}.json",
            )

        os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else ".", exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(list(unique_papers.values()), f, ensure_ascii=False, indent=2)
        log(f"💾 File saved to: {output_file}")
    else:
        log("⚠️ No papers found. Check your date range or network.")
    if max_published_new:
        save_seen_state(seen_ids, max_published_new)
    else:
        save_seen_state(seen_ids, latest_published_at)
    save_last_crawl_at(end_date)
    group_end()

if __name__ == "__main__":
    # 建议先用 days=1 测试一下，没问题再跑更长时间窗口
    fetch_all_domains_metadata_robust()
