import json
import logging
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

es = Elasticsearch("http://10.3.24.46:9200", basic_auth=("elastic", "sciyon"), verify_certs=False)

# print(es.indices.get_mapping(index=es_index_name))
print("***********")


def storage_api2es(each_item):
    # ES数据库中创建具有向量数据的索引
    EMBEDDING_DIM = 1024  # bge-zh-1.5 输出维度
    es_index_name = "ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_api"
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
                        "id": {"type": "keyword"},
                        "kb_id": {"type": "keyword"},
                        "name": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "api_description": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart"
                        },
                        "api_config": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "embeded_type": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "vector": {
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
    for item in each_item.values():
        api_id = item["id"]
        kb_id = item["kb_id"]
        name = str(item["name"])
        api_description = str(item["api_description"])
        api_config = str(item["api_config"])
        embeded_type = item["embeded_type"]
        qv_vector = item["vector"]

        doc = {
            "id": api_id,
            "kb_id": kb_id,
            "name": name,
            "api_description": api_description,
            "api_config": api_config,
            "embeded_type": embeded_type,
            "vector": qv_vector
        }
        actions.append({"_index": es_index_name, "_id": api_id, "_source": doc})

    bulk(es, actions)
    print(f"✅ 成功写入 {len(actions)} 条文档到 Elasticsearch")


def storage_page2es(each_item):
    # ES数据库中创建具有向量数据的索引
    EMBEDDING_DIM = 1024  # bge-zh-1.5 输出维度
    es_index_name = "ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_page"
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
                        "page_id": {"type": "keyword"},
                        "page_name": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "page_description": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart"
                        },
                        "page_url": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "params": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "embeded_type": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "vector": {
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
    for item in each_item.values():
        page_id = item["page_id"]
        page_name = str(item["page_name"])
        page_description = str(item["page_description"])
        page_url = item["page_url"]
        params = item["params"]
        embeded_type = item["embeded_type"]
        qv_vector = item["vector"]

        doc = {
            "page_id": page_id,
            "page_name": page_name,
            "page_description": page_description,
            "page_url": page_url,
            "params": params,
            "embeded_type": embeded_type,
            "vector": qv_vector
        }
        actions.append({"_index": es_index_name, "_id": page_id, "_source": doc})

    bulk(es, actions)
    print(f"✅ 成功写入 {len(actions)} 条文档到 Elasticsearch")


def storage_jkdata2es(each_item):
    # ES数据库中创建具有向量数据的索引
    EMBEDDING_DIM = 1024  # bge-zh-1.5 输出维度
    es_index_name = "ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_jkdata"
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
                        "id": {"type": "keyword"},
                        "name": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "company_name": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "description": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart"
                        },
                        "fields_info": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "has_main_column": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "main_column_info": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_smart",
                            "fields": {
                                "keyword": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "vector_name": {
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
    for item in each_item.values():
        data_id = item["id"]
        name = str(item["name"])
        company_name = item["company_name"]
        description = str(item["description"])
        fields_info = str(item["fields_info"])
        has_main_column = item["has_main_column"]
        main_column_info = item["main_column_info"]
        qv_vector = item["vector_name"]

        doc = {
            "id": data_id,
            "company_name": company_name,
            "name": name,
            "description": description,
            "fields_info": fields_info,
            "has_main_column": has_main_column,
            "main_column_info": main_column_info,
            "vector_name": qv_vector
        }
        actions.append({"_index": es_index_name, "_id": data_id, "_source": doc})

    bulk(es, actions)
    print(f"✅ 成功写入 {len(actions)} 条文档到 Elasticsearch")


def update_api_info(new_doc):
    es_index_name = "ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_api"
    # 使用 update + upsert
    for item in new_doc.values():
        # 注意这里：api_config 要转成 JSON 字符串
        item["api_config"] = json.dumps(item["api_config"], ensure_ascii=False)
        current_id = item["id"]
        resp = es.update(
            index=es_index_name,
            id=current_id,  # 用id作为文档ID，保证唯一性
            body={
                # "doc": new_doc,  # 更新的内容
                "doc": item,  # 更新的内容
                "doc_as_upsert": True  # 如果不存在则插入
            }
        )

    print("更新或插入结果：", resp)


def delete_by_id(es_index_name, _id):
    query = {
        "query": {
            "term": {"id": "689342775065313280"}
        }
    }
    resp = es.search(index=es_index_name, body=query)
    print(resp)
    es.delete(index=es_index_name, id=_id)
    logging.info("删除成功！")


def delete_single_index(es_index_name):
    # 删除整个索引
    if es.indices.exists(index=es_index_name):
        es.indices.delete(index=es_index_name)
        print(f"索引 '{es_index_name}' 已成功删除。")
    else:
        print(f"索引 '{es_index_name}'不存在")


def update_add_fields(es_index_name):
    # 默认给page数据集添加字段
    mapping_update = {
        "properties": {
            "company_name": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}},
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_smart"
            },
            "company_infomation": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}},
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_smart"
            }
        }
    }

    # 更新映射
    es.indices.put_mapping(index=es_index_name, body=mapping_update)
    print("✅ 已成功更新索引映射。")


def update_add_fields_values(es_index_name):
    doc_id = "690682229338275840"

    update_body = {
        "doc": {
            "company_name": "alibaba",
            "company_infomation": "位于浙江省杭州市"
        }
    }

    # 更新单个文档
    es.update(index=es_index_name, id=doc_id, body=update_body)
    print(f"✅ 文档 {doc_id} 已更新。")


def delete_fields_values(es_index_name):
    # 使用 painless 脚本删除字段
    delete_fields_script = {
        "script": {
            "source": """
                if (ctx._source.containsKey('company_name')) {
                    ctx._source.remove('company_name');
                }
                if (ctx._source.containsKey('company_infomation')) {
                    ctx._source.remove('company_infomation');
                }
            """
        }
    }

    # 批量更新：删除所有文档中的字段
    response = es.update_by_query(index=es_index_name, body=delete_fields_script, conflicts="proceed")
    print(f"✅ 已从所有文档中删除字段数据。\n修改的文档数: {response['updated']}")
if __name__ == '__main__':
    # delete_single_index("ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_new")
    # delete_single_index("ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_jkdata")
    # delete_by_id("ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_jkdata","686178457968508928")
    # update_add_fields("ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_page")
    update_add_fields_values("ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_page")
    # delete_fields_values("ragflow_sciyonff8bcdc11efbdcf88aedd6333bi_page")