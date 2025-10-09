from flask import Flask, jsonify
import requests
import json
from BI_data.utils.esconn import update_api_info
from BI_data.utils.embedde_utils import get_embedding

app = Flask(__name__)

@app.route('/sync_apis', methods=['GET'])
def sync_apis():
    try:
        # 请求地址（可考虑通过参数传入 ids，这里先写死）
        url = "http://10.44.2.104:9090/mainApi/syncplant-business-rag-wangchao/api/api/listAll?ids=987654321987654321"

        # 请求头
        headers = {
            "sciyon-auth": "bearer 570822ee-6bb8-4730-9657-1fcecb0c214d"
        }

        # 发送 GET 请求
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            return jsonify({
                "error": "Failed to fetch data from remote API",
                "status_code": response.status_code,
                "response": response.text
            }), 500

        try:
            response_data = response.json()
        except json.JSONDecodeError as e:
            return jsonify({
                "error": "Invalid JSON response from remote API",
                "exception": str(e),
                "raw_response": response.text
            }), 500

        # 解析 config 字段
        for item in response_data.get("data", []):
            try:
                if "config" in item and isinstance(item["config"], str):
                    item["config"] = json.loads(item["config"])
            except (json.JSONDecodeError, TypeError) as e:
                print(f"Warning: Failed to parse config for item id={item.get('id')}: {e}")

        all_data = response_data.get("data", [])
        if not all_data:
            return jsonify({"message": "No data returned from API"}), 200

        # 生成嵌入并构造数据
        each_item = {}
        for item in all_data:
            try:
                qv_vector = get_embedding(item["name"])
                # 确保嵌入结果有效
                if not qv_vector or not hasattr(qv_vector[0], 'embedding'):
                    return jsonify({
                        "error": "Failed to generate embedding for item",
                        "item_id": item.get("id"),
                        "name": item.get("name")
                    }), 500

                each_item[item["id"]] = {
                    "id": item["id"],
                    "kb_id": item["knowledgebaseId"],
                    "name": item["name"],
                    "api_description": item["description"],
                    "api_config": item["config"],
                    "embeded_type": "api_name",
                    "vector": qv_vector[0].embedding
                }
            except Exception as e:
                return jsonify({
                    "error": "Error processing item",
                    "item_id": item.get("id"),
                    "exception": str(e)
                }), 500

        # 写入 Elasticsearch
        update_api_info(each_item)

        return jsonify({
            "message": "API data synced successfully",
            "total_items": len(each_item)
        }), 200

    except Exception as e:
        # 记录异常（生产环境建议用 logging）
        print("Exception in /sync_apis:", str(e))
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)