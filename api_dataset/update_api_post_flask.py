from flask import Flask, request, jsonify
import requests
import json
from BI_data.utils.esconn import update_api_info
from BI_data.utils.embedde_utils import get_embedding
import os
from dotenv import load_dotenv

# 加载 .env 文件（开发环境）
load_dotenv()

# 现在可以从环境变量中安全读取 token
AUTH_TOKEN = os.getenv("SCIYON_AUTH_TOKEN")
app = Flask(__name__)

@app.route('/sync_apis', methods=['POST'])
def sync_apis():
    try:
        # 从 JSON body 中获取 ids
        data = request.get_json()
        if not data or 'ids' not in data:
            return jsonify({"error": "Missing 'ids' in request body"}), 400

        ids = data['ids']
        # 支持字符串（如 "123,456"）或列表（如 [123, 456]）
        if isinstance(ids, list):
            ids_str = ",".join(str(x) for x in ids)
        elif isinstance(ids, str):
            ids_str = ids.strip()
        else:
            return jsonify({"error": "'ids' must be a string or list"}), 400

        if not ids_str:
            return jsonify({"error": "'ids' is empty"}), 400

        # 构造目标 URL
        base_url = "http://10.44.2.104:9090/mainApi/syncplant-business-rag-wangchao/api/api/listAll"
        url = f"{base_url}?ids={ids_str}"

        headers = {
            "sciyon-auth": AUTH_TOKEN
        }

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            return jsonify({
                "error": "Remote API request failed",
                "status_code": response.status_code,
                "response": response.text
            }), 500

        try:
            response_data = response.json()
        except json.JSONDecodeError as e:
            return jsonify({
                "error": "Invalid JSON from remote API",
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
            return jsonify({"message": "No data returned from remote API", "ids": ids_str}), 200

        # 生成嵌入
        each_item = {}
        for item in all_data:
            try:
                qv_vector = get_embedding(item["name"])
                if not qv_vector or not hasattr(qv_vector[0], 'embedding'):
                    raise ValueError("Embedding generation failed")

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
                    "error": f"Failed to process item {item.get('id')}",
                    "exception": str(e)
                }), 500

        # 写入 ES
        update_api_info(each_item)

        return jsonify({
            "message": "API data synced successfully",
            "ids_requested": ids_str,
            "total_items_processed": len(each_item)
        }), 200

    except Exception as e:
        print("Exception in /sync_apis:", str(e))
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)