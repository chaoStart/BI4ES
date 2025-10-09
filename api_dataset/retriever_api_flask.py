
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

def selfmerge(sres):
    uniqueKeywordsRes = {}
    keyword_res = []
    for item in sres:
        key = item["_source"]["kb_id"] + "|" + item["_source"]["id"]
        if key not in uniqueKeywordsRes:
            uniqueKeywordsRes[key] = item
            keyword_res.append(item)
    return keyword_res

def hybrid_search_indicator(query_text, query_vector, kb_id, es_index_name="ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_api"):
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
                {"term": {"kb_id": kb_id}}
            ]
        }
    }
    keyword_res = es.search(index=es_index_name, query=keyword_query, size=10)["hits"]["hits"]
    keyword_res = selfmerge(keyword_res)
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
                "boost_mode": "replace"
            }
        },
        "size": 10
    }
    vector_res = es.search(index=es_index_name, body=vector_query)["hits"]["hits"]
    vector_res = selfmerge(vector_res)

    # ====== 3. 合并逻辑 ======
    merged = {}
    for hit in keyword_res:
        key = hit["_source"]["id"]
        merged[key] = {
            "source": hit["_source"],
            "bm25_score": hit["normalized_score"],
            "vector_score": 0.0
        }

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

    final_results = []
    for key, val in merged.items():
        bm25 = val["bm25_score"]
        vec = val["vector_score"]

        if bm25 > 0 and vec > 0:
            score = bm25 * 0.5 + vec * 0.5
        else:
            score = max(bm25, vec) * 0.5

        final_results.append({
            "score": score,
            "api_id": val["source"]["id"],
            "kb_id": val["source"]["kb_id"],
            "name": val["source"]["name"],
            "description": val["source"]["api_description"],
            "source": val["source"]
        })

    final_results.sort(key=lambda x: x["score"], reverse=True)
    return final_results[:10]


def get_high_score(results_list):
    if not results_list:
        return []
    score1 = results_list[0]["score"]
    record_high_score = []
    for item in results_list:
        if (score1 - item["score"]) / score1 <= 0.5:
            record_high_score.append(item)
    return record_high_score


@app.route('/search', methods=['POST'])
def search():
    try:
        data = request.get_json()
        query_text = data.get("query_text")
        kb_id = data.get("kb_id", "685598654500634625")  # 默认 kb_id

        if not query_text:
            return jsonify({"error": "Missing 'query_text' in request body"}), 400

        # 获取嵌入向量
        query_vector = get_embedding(query_text)

        # 执行混合检索
        results = hybrid_search_indicator(query_text, query_vector, kb_id)

        # 过滤高分结果
        high_score_results = get_high_score(results)

        # 构造返回格式
        response = []
        for item in high_score_results:
            response.append({
                "name": item["name"],
                "api_id": item["api_id"],
                "description": item["description"],
                "score": item["score"]
            })

        return jsonify({
            "query_text": query_text,
            "kb_id": kb_id,
            "results": response,
            "total": len(response)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)