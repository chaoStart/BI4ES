
from flask import Flask, request, jsonify
from BI_data.utils.embedde_utils import get_embedding
from BI_data.utils.esconn import es

# 初始化 Flask 应用
app = Flask(__name__)

def normalization(sres):
    if not sres:
        return sres
    score_list = [item["_score"] for item in sres]
    max_score = max(score_list)
    min_score = min(score_list)
    scale = max_score - min_score
    if scale != 0:
        for item in sres:
            item["normalized_score"] = (max_score - item["_score"]) / scale
    else:
        for item in sres:
            item["normalized_score"] = 1.0
    return sres

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

def hybrid_search_indicator(query_text, query_vector, id="691222347303550994", es_index_name="ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_page" ):
    # ====== 1. 关键词检索 ======
    keyword_query = {
        "bool": {
            "must": [
                {
                    "multi_match": {
                        "query": query_text,
                        "fields": ["page_name", "page_description"],
                        "type": "best_fields",
                        "analyzer": "ik_smart"
                    }
                }
            ],
            "filter": [
                {
                    "term": {
                        "page_id": id
                    }
                }
            ]
        }
    }
    keyword_res = es.search(index=es_index_name, query=keyword_query, size=10)["hits"]["hits"]

    # 调用归一化方法
    keyword_res = normalization(keyword_res)

    # ====== 2. 向量检索 ======
    vector_query = {
    "query": {
        "function_score": {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"page_id": id}},
                        {"exists": {"field": "vector_name"}}
                    ]
                }
            },
            "script_score": {
                "script": {
                    "source": "(cosineSimilarity(params.query_vector, 'vector_name') + 1.0) / 2",
                    "params": {"query_vector": query_vector[0].embedding}
                }
            },
            "boost_mode": "replace"  # 可选：用脚本分数完全替代原始分数
        }
    },
    "size": 10
}

    vector_res = es.search(index=es_index_name, body=vector_query)["hits"]["hits"]
    # ====== 3. 合并逻辑 ======
    merged = {}
    # 先存放关键词检索结果
    for hit in keyword_res:
        key = hit["_source"]["page_id"]
        merged[key] = {
            "source": hit["_source"],
            # "bm25_score": hit["_score"],
            "bm25_score": hit["normalized_score"],
            "vector_score": 0.0  # 暂无
        }

    # 再存放向量检索结果
    for hit in vector_res:
        key = hit["_source"]["page_id"]
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
            "page_id": val["source"]["page_id"],
            "page_name": val["source"]["page_name"],
            "page_description": val["source"]["page_description"],
            "source": val["source"]
        })

    # 排序
    final_results.sort(key=lambda x: x["score"], reverse=True)
    print("关键词+向量召回的指标数据总数:", len(final_results))
    # 取前10个
    top_10_results = final_results[:10]
    return top_10_results


@app.route('/search_page', methods=['POST'])
def search():
    try:
        data = request.get_json()
        query_text = data.get("query_text")
        page_id = data.get("page_id", "690682229338275840")

        if not query_text:
            return jsonify({"error": "Missing 'query_text' in request body"}), 400

        # 获取嵌入向量
        query_vector = get_embedding(query_text)

        # 执行混合检索
        results = hybrid_search_indicator(query_text, query_vector, page_id)

        # 过滤高分结果
        high_score_results = get_high_score(results)

        # 构造返回格式
        response = []
        for item in high_score_results:
            response.append({
                "page_id": item["page_id"],
                "page_name": item["page_name"],
                "page_description": item["page_description"],
                "score": item["score"]
            })

        return jsonify({
            "query_text": query_text,
            "id": page_id,
            "results": response,
            "total": len(response)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)