import arxiv
import json
import os
import time
from datetime import datetime, timedelta, timezone

# 项目根目录（当前脚本位于 scripts/ 下）
SCRIPT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

# ArXiv 的主要一级分类列表
# 注意：物理学比较特殊，ArXiv 历史上有很多独立的物理存档，为了保险，我们列出主要的
CATEGORIES_TO_FETCH = [
    "cs", "math", "stat", "q-bio", "q-fin", "eess", "econ",
    "physics", "cond-mat", "hep-ph", "hep-th", "gr-qc", "astro-ph",
]

def log(message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def group_start(title: str) -> None:
    print(f"::group::{title}", flush=True)


def group_end() -> None:
    print("::endgroup::", flush=True)


def fetch_all_domains_metadata_robust(
    days: int = 1,
    output_file: str | None = None,
) -> None:
    # 1. 计算时间窗口
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)
    
    start_str = start_date.strftime("%Y%m%d0000")
    end_str = end_date.strftime("%Y%m%d2359")
    
    group_start("Step 1 - fetch arXiv")
    log(f"🌍 [Global Ingest] Window: {start_str} TO {end_str}")
    
    # 结果集使用字典去重 (因为有些论文跨领域，比如同时在 cs 和 stat)
    unique_papers = {}
    
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
            today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
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
    group_end()

if __name__ == "__main__":
    # 建议先用 days=1 测试一下，没问题再跑更长时间窗口
    fetch_all_domains_metadata_robust(days=3)
