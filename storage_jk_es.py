
import json
import requests
from BI_data.utils.embedde_utils import get_embedding
# 1. 请求地址
URL = "http://10.44.2.104:9090/mainApi/syncplant-business-dataset/api/empoworx/dataset/rag/getDatasetConfigInfo"

# 2. 请求头（按需调整）
HEADERS = {
    "Content-Type": "application/json;charset=utf-8",
    "Accept": "application/json, text/plain, */*",
    # "Authorization": "Bearer YOUR_TOKEN_HERE",   # 如需 Token
    # "Cookie": "JSESSIONID=xxx"                   # 如需 Cookie
}

# 3. 请求体（直接复用题目中的 JSON）
PAYLOAD = [
    "674192982357246731",
    "690635663360491541",
    "634255584696598528",
    "686178457968508928",
    "683297643013472330",
    "689735738862141648",
    "675769003428380695",
    "691222347303550994",
    "646026732849070083"
]

def fetch_dataset(url: str, payload: dict, headers: dict, timeout: int = 30):
    """
    发送 POST 请求并返回 JSON
    """
    try:
        resp = requests.post(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers=headers,
            timeout=timeout,
            # auth=("user", "pass")  # 如需 BasicAuth
        )
        resp.raise_for_status()          # 非 2xx 会抛异常
        return resp.json()               # 若返回非 JSON 会抛 ValueError
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] 网络层异常: {e}")
    except ValueError as e:
        print(f"[ERROR] 返回非合法 JSON: {e}")
    return None


response_data = fetch_dataset(URL, PAYLOAD, HEADERS)
all_data = response_data["data"]
print(all_data)

# 数据集基本描述信息
data_description = all_data.get("description", "这是测试的数据集基本信息描述")
# 数据集的dataId信息
data_id = all_data["dsBasedatasetDto"].get("id")
# 数据集的主列信息（如，营业额总收入等）
data_item_content = []
for i, item in enumerate(all_data["data"]):
    data_item_content.append(item["cell30"])

# 数据集描述信息的向量数据
description_vector = get_embedding(data_description)[0].embedding

# 数据集字段的向量数据
fields_embedding_list = get_embedding(data_item_content)

data_info = {}

#  存储到ES数据库中
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
es = Elasticsearch("http://10.3.24.46:9200", basic_auth=("elastic", "sciyon"), verify_certs=False)
es_index_name = "ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_new"

for index, value in enumerate(data_item_content, start=0):
    data_info[index] = {
        "data_id": data_id,
        "data_description": data_description,
        "embedding_type": "content",
        'content': data_item_content[index],
        "content_embedding": fields_embedding_list[index].embedding,
    }

# ES数据库中创建具有向量数据的索引
EMBEDDING_DIM = 1024  # bge-zh-1.5 输出维度

if not es.indices.exists(index=es_index_name):
    es.indices.create(
        index=es_index_name,
        body={
            "settings": {
                "number_of_shards": 1,
                "analysis": {
                    "analyzer": {
                        "ik_max_word_analyzer": {
                            "type": "custom",
                            "tokenizer": "ik_max_word"
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "data_id": {"type": "keyword"},
                    "data_description": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
                    },
                    "embedding_type": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
                    },
                    "content": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "content_embedding": {
                        "type": "dense_vector",
                        "dims": EMBEDDING_DIM,
                        "index": True,
                        "similarity": "cosine"
                    }
                }
            }
        }
    )
    print("✅ 已创建支持向量和 IK 分词器的索引")
else:
    print("⚠️ 索引已存在，跳过创建")


# ES数据库中存储文本数据和向量数据
actions = []
for item in data_info.values():
    data_id = data_id
    data_description = str(item["data_description"])
    fields_type = item["embedding_type"]
    content = str(item["content"])
    embedding = item["content_embedding"]

    doc = {
        "data_id": data_id,
        "data_description": data_description,
        "embedding_type": fields_type,
        "content": content,
        "content_embedding": embedding
    }
    actions.append({"_index": es_index_name, "_source": doc})

bulk(es, actions)
print(f"✅ 成功写入 {len(actions)} 条文档到 Elasticsearch")


if __name__ == "__main__":
    result = fetch_dataset(URL, PAYLOAD, HEADERS)
    if result:
        # 美化打印
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print("未能获取到数据，请检查网络、地址或认证信息。")