
import requests
import json
from BI_data.utils.esconn import storage_api2es
from BI_data.utils.embedde_utils import get_embedding
import os
from dotenv import load_dotenv

# 加载 .env 文件（开发环境）
load_dotenv()

# 现在可以从环境变量中安全读取 token
AUTH_TOKEN = os.getenv("SCIYON_AUTH_TOKEN")
BASE_URL = os.getenv("BASE_URL")
# 请求地址
# url = "http://10.44.2.104:9090/mainApi/syncplant-business-rag-wangchao/api/api/listAll"

# 请求头
headers = {
    # "sciyon-auth": "bearer 77d58af8-924f-408e-88a4-ad1bb8ef81ac"
    "sciyon-auth": AUTH_TOKEN
}

# 发送 GET 请求
response = requests.get(BASE_URL, headers=headers)
api_datasets = response.text
print("APi的数据类型", type(api_datasets))

# 检查响应状态
if response.status_code == 200:
    # 第一次解析：将整个响应体转为 Python 字典
    try:
        response_data = response.json()  # 等价于 json.loads(response.text)
    except json.JSONDecodeError as e:
        print("响应不是有效的 JSON 格式:", e)
        exit(1)

    # 遍历 data 列表，对每个 item 的 config 字段进行二次解析
    for item in response_data.get("data", []):
        try:
            # 将 config 字符串解析为字典
            item["config"] = json.loads(item["config"])
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            print(f"解析 config 字段失败: {e}, 原始值: {item.get('config')}")

    # 此时 result 是完全结构化的数据，可以安全使用
    print("解析成功！示例数据（前两个字段）：")

else:
    print(f"请求失败，状态码: {response.status_code}")
    print("响应内容:", response.text)


all_data = response_data["data"]
print(all_data)


each_item = {}
for item in all_data:
    key = item["id"]
    qv_vector =get_embedding(item["name"])
    # qv_vector =get_embedding(item["description"])
    each_item[key] = {
        "id": item["id"],
        "name": item["name"],
        "description": item["description"],
        "api_config": item["config"],
        "embeded_type": "api_name",
        "vector": qv_vector[0].embedding
    }

# 把读取的数据存储到ES数据库中
storage_api2es(each_item)
