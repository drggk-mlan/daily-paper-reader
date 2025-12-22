#!/usr/bin/env python
"""
基于 Zotero 相似度的论文抓取脚本

思路：
1. 从 Zotero 拉取个人论文库（作为偏好语料库 corpus）
2. 使用 daily_job.search_arxiv_today 获取一批候选 Arxiv 论文
3. 使用 SentenceTransformer 将「Zotero 摘要」与「候选论文摘要」编码为向量
4. 结合时间衰减权重计算相似度分数，对候选论文排序
5. 选取 Top N 篇，复用 daily_job.process_single_paper 生成 docs/*.md / .txt / .pdf 并更新侧边栏

配置约定：
- 环境变量 ZOTERO_ID：Zotero User ID
- 环境变量 ZOTERO_API_KEY：Zotero API Key
- 环境变量 ZOTERO_MAX_PAPERS：每日最多生成多少篇（默认 10）
- 环境变量 ZOTERO_MODEL：用于相似度的 embedding 模型（默认 avsolatorio/GIST-small-Embedding-v0）
"""

import os
import sys
import datetime
from typing import List, Dict

import numpy as np
from pyzotero import zotero as zotero_client
from sentence_transformers import SentenceTransformer

import daily_job


def get_zotero_corpus(user_id: str, api_key: str) -> List[Dict]:
    """
    从 Zotero 拉取语料库：
    - 只保留有摘要（abstractNote）的条目
    - 不做 collection 过滤（如需过滤，可后续扩展）
    返回 Zotero 原始 item 列表，每个元素是一个 dict。
    """
    zot = zotero_client.Zotero(user_id, "user", api_key)
    corpus = zot.everything(
        zot.items(itemType="conferencePaper || journalArticle || preprint")
    )
    corpus = [
        c for c in corpus if c.get("data", {}).get("abstractNote", "").strip() != ""
    ]
    return corpus


def _parse_zotero_date_added(item: Dict) -> datetime.datetime:
    """
    解析 Zotero item 的 dateAdded 字段，格式一般为：YYYY-MM-DDTHH:MM:SSZ
    解析失败时，返回一个较早的默认时间，避免影响排序。
    """
    date_str = item.get("data", {}).get("dateAdded") or ""
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        # 兜底：给一个很早的时间
        return datetime.datetime(2000, 1, 1)


def rerank_papers_by_zotero_similarity(
    candidates: List[Dict],
    corpus: List[Dict],
    model_name: str = "avsolatorio/GIST-small-Embedding-v0",
) -> List[Dict]:
    """
    使用 SentenceTransformer 基于「摘要」计算候选论文与 Zotero 语料库的相似度，并按分数排序。

    评分规则：
    - 对 corpus 按 dateAdded 从新到旧排序
    - 对较新的条目给予更高的时间权重（对数衰减）
    - 相似度为 cos_sim(cand, corpus) 按加权求和
    - 最终分数乘以 10 方便阅读，并写入 candidate['zotero_score']
    """
    if not candidates or not corpus:
        return candidates

    encoder = SentenceTransformer(model_name)

    # 按时间从新到旧排序，并计算时间衰减权重
    corpus_sorted = sorted(
        corpus, key=_parse_zotero_date_added, reverse=True
    )
    n_corpus = len(corpus_sorted)
    indices = np.arange(n_corpus) + 1  # 1,2,...,n
    time_decay_weight = 1.0 / (1.0 + np.log10(indices))
    time_decay_weight = time_decay_weight / time_decay_weight.sum()  # 归一化

    # 准备文本：Zotero 用 abstractNote，候选论文用 arxiv 摘要（summary）
    corpus_texts = [
        c.get("data", {}).get("abstractNote", "") for c in corpus_sorted
    ]
    candidate_texts = [
        (c.get("summary") or c.get("title") or "") for c in candidates
    ]

    # 编码为向量，并做 L2 归一化，便于使用点积近似 cos 相似度
    corpus_vecs = encoder.encode(
        corpus_texts,
        batch_size=32,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )
    cand_vecs = encoder.encode(
        candidate_texts,
        batch_size=32,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )

    # 相似度矩阵：[n_cand, n_corpus]
    sim = np.matmul(cand_vecs, corpus_vecs.T)

    # 按时间衰减权重做加权求和，得到每篇候选论文的总体分数
    scores = (sim * time_decay_weight.reshape(1, -1)).sum(axis=1) * 10.0

    for s, c in zip(scores, candidates):
        c["zotero_score"] = float(s)

    candidates_sorted = sorted(
        candidates, key=lambda x: x.get("zotero_score", 0.0), reverse=True
    )
    return candidates_sorted


def main():
    # 1. 读取 Zotero 配置
    zotero_id = os.getenv("ZOTERO_ID")
    zotero_key = os.getenv("ZOTERO_API_KEY")

    if not zotero_id or not zotero_key:
        print(
            "[ERROR] 缺少 ZOTERO_ID 或 ZOTERO_API_KEY 环境变量，请在 .env 中配置后重试。"
        )
        sys.exit(1)

    max_papers = int(os.getenv("ZOTERO_MAX_PAPERS", "10"))
    model_name = os.getenv(
        "ZOTERO_MODEL", "avsolatorio/GIST-small-Embedding-v0"
    )

    # 2. 获取 Zotero 语料库
    print("[INFO] 从 Zotero 拉取语料库...")
    corpus = get_zotero_corpus(zotero_id, zotero_key)
    if not corpus:
        print("[WARN] Zotero 中没有带摘要的论文条目，无法进行相似度排序。")
        sys.exit(0)
    print(f"[INFO] Zotero 语料库大小：{len(corpus)} 篇。")

    # 3. 获取候选 Arxiv 论文：沿用 daily_job 的关键词逻辑
    keywords = daily_job.get_keywords()
    print(f"[INFO] 使用关键词（OR 逻辑）获取候选论文：{keywords}")
    candidates = daily_job.search_arxiv_today(keywords, max_results=100)
    if not candidates:
        print("[INFO] 没有找到新的候选论文。")
        sys.exit(0)
    print(f"[INFO] 候选论文数量：{len(candidates)} 篇。")

    # 4. 基于 Zotero 相似度重排
    print(
        f"[INFO] 使用模型 {model_name} 基于 Zotero 相似度对候选论文进行重排..."
    )
    ranked = rerank_papers_by_zotero_similarity(
        candidates, corpus, model_name=model_name
    )

    # 5. 选取 Top N，并复用 daily_job 的处理逻辑生成 docs
    selected = ranked[:max_papers]
    print(
        f"[INFO] 选取前 {len(selected)} 篇论文，生成本地 Markdown / PDF / TXT..."
    )
    for idx, paper in enumerate(selected, start=1):
        title = paper.get("title", "Untitled")
        score = paper.get("zotero_score", 0.0)
        print(f"[INFO] 处理第 {idx} 篇：{title}（Zotero 相似度得分：{score:.3f}）")
        try:
            daily_job.process_single_paper(paper, source="zotero")
        except Exception as e:
            print(f"[ERROR] 处理论文失败：{title}，原因：{e}")

    print("[INFO] 基于 Zotero 相似度的论文抓取完成。")


if __name__ == "__main__":
    main()
