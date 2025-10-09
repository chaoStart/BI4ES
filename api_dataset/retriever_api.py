
from elasticsearch import Elasticsearch
from BI_data.utils.embedde_utils import get_embedding
es = Elasticsearch("http://10.3.24.46:9200", basic_auth=("elastic", "sciyon"), verify_certs=False)
index_name = "ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_api"

print(es.indices.get_mapping(index=index_name))


def normalization(sres):
    score_list = []
    for item in sres:
        score_list.append(item["_score"])
    max_score = max(score_list)
    min_score = min(score_list)
    scale = max_score-min_score
    if max_score != min_score:
        for item in sres:
            item["normalized_score"] = (max_score-item["_score"])/scale
    else:
        for item in sres:
            item["normalized_score"] = 1.0
    return sres


# 过滤ES返回数据中的低分数据
def get_high_score(results_list):
    # 获取高分行召回数据
    record_high_score = []
    # 获取第一个得分最高最相关的数据
    score1 = results_list[0]["score"]
    for index, item in enumerate(results_list):
        score_x = results_list[index]["score"]
        if (score1 - score_x) / score1 <= 0.5:
            record_high_score.append(item)
    return record_high_score


def selfmerge(sres):
    keyword_res = []
    uniqueKeywordsRes = {}
    for item in sres:
        key = item["_source"]["kb_id"] +"|" + item["_source"]["id"]
        if key not in uniqueKeywordsRes:
            uniqueKeywordsRes[key] = item
            keyword_res.append(item)
    return keyword_res


def hybrid_search_indicator(query_text, query_vector, kb_id, id=None):
    # ====== 1. 关键词检索 ======
    keyword_query = {
        "bool": {
            "must": [
                {
                    "multi_match": {
                        "query": query_text,
                        "fields": ["name", "api_description"],
                        "type": "best_fields",
                        "analyzer": "ik_smart"
                    }
                }
            ],
            "filter": [
                {
                    "term": {
                        "kb_id": kb_id
                    }
                }
            ]
        }
    }
    keyword_res = es.search(index=index_name, query=keyword_query, size=10)["hits"]["hits"]

    # 文本检索到的数据进行合并（相同kb_id和id的合并在一起）（由于存储的时候，相同id,kb_id对应的存储嵌入类型不一样）
    keyword_res = selfmerge(keyword_res)

    # 调用归一化方法
    keyword_res = normalization(keyword_res)

    # ====== 2. 向量检索 ======
    vector_query = {
    "query": {
        "function_score": {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"kb_id": kb_id}},
                        {"exists": {"field": "vector"}}
                    ]
                }
            },
            "script_score": {
                "script": {
                    "source": "(cosineSimilarity(params.query_vector, 'vector') + 1.0) / 2",
                    "params": {"query_vector": query_vector[0].embedding}
                }
            },
            "boost_mode": "replace"  # 可选：用脚本分数完全替代原始分数
        }
    },
    "size": 10
}

    vector_res = es.search(index=index_name, body=vector_query)["hits"]["hits"]

    # 向量结果进行合并（相同的id和kb_id进行合并）
    vector_res = selfmerge(vector_res)
    # ====== 3. 合并逻辑 ======
    merged = {}
    # 先存放关键词检索结果
    for hit in keyword_res:
        key = hit["_source"]["id"]
        merged[key] = {
            "source": hit["_source"],
            # "bm25_score": hit["_score"],
            "bm25_score": hit["normalized_score"],
            "vector_score": 0.0  # 暂无
        }

    # 再存放向量检索结果
    for hit in vector_res:
        key = hit["_source"]["id"]
        if key in merged:
            merged[key]["vector_score"] = hit["_score"]
        else:
            merged[key] = {
                "source": hit["_source"],
                "bm25_score": 0.0,
                "vector_score": hit["_score"]
            }

    # 计算新得分
    final_results = []
    for key, val in merged.items():
        bm25 = val["bm25_score"]
        vec = val["vector_score"]

        if bm25 > 0 and vec > 0:  # 同时命中
            score = bm25 * 0.5 + vec * 0.5
        else:  # 只命中一个
            score = max(bm25, vec) * 0.5

        final_results.append({
            "score": score,
            "api_id": val["source"]["id"],
            "kb_id": val["source"]["kb_id"],
            "name": val["source"]["name"],
            "description": val["source"]["api_description"],
            "source": val["source"]
        })

    # 排序
    final_results.sort(key=lambda x: x["score"], reverse=True)
    print("关键词+向量召回的指标数据总数:", len(final_results))
    # 取前10个
    top_10_results = final_results[:10]
    return top_10_results


# 用户输入问题
query_text = "获取知识库列表"

query_vector = get_embedding(query_text)

# 检索ES数据
results = hybrid_search_indicator(query_text, query_vector, kb_id="685598654500634625")


high_indicator_res = get_high_score(results)


# 打印结果
for v in high_indicator_res:
    n = v["name"]
    api_id = v["api_id"]
    des = v["description"]
    print(f"数据集名称Name: {n} - 数据集ID:{api_id} - 数据集描述Description: {des}")
