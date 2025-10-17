import ast
from BI_data.utils.esconn import storage_jkdata2es
from BI_data.utils.embedde_utils import get_embedding
from BI_data.utils.getdatainfo import recursion_row_chidren_all

import json
import requests
import os
from dotenv import load_dotenv

# 加载 .env 文件（开发环境）
load_dotenv()

# 现在可以从环境变量中安全读取 token
AUTH_TOKEN = os.getenv("SCIYON_AUTH_TOKEN")
BASE_URL = os.getenv("BASE_URL")
# 1. 请求地址
# URL = "http://10.44.2.104:9090/mainApi/syncplant-business-dataset/api/empoworx/dataset/rag/getDatasetConfigInfo"

# 2. 请求头（按需调整）
HEADERS = {
    "Content-Type": "application/json;charset=utf-8",
    "Accept": "application/json, text/plain, */*",
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


response_data = fetch_dataset(BASE_URL, PAYLOAD, HEADERS)
all_data = response_data["data"]


each_item = {}
for item in all_data.items():
    key = item[1]["baseInfo"]["id"]
    name = item[1]["baseInfo"]["name"]
    company_list = ast.literal_eval(item[1].get("数据集公司", "[]"))
    company_name = company_list[-1] if len(company_list) else " "
    description = item[1]["baseInfo"].get("description", " ")
    fields_info = " ".join([item[1]["columnList"][i]["columnName"] for i in range(len(item[1]["columnList"]))])
    has_main_column = item[1]["主列"]
    # 获取主列信息
    row_list_data = item[1]["data"]
    if len(has_main_column) and len(row_list_data):
        name_list = {}  # 节点行数据集和
        for i in range(len(row_list_data)):
            node_leaf = []
            recursion_row_chidren_all(row_list_data[i], node_leaf)
            node_content = "\n".join(node_leaf)
            name_list[row_list_data[i]["path"]] = node_content
    main_column_info = "\n".join(name_list.values()) if has_main_column else " "
    qv_vector = get_embedding(name)
    each_item[key] = {
        "id": key,
        "name": name,
        "company_name": company_name,
        "description": description,
        "fields_info": fields_info,
        "has_main_column": has_main_column,
        "main_column_info": main_column_info,
        "vector": qv_vector[0].embedding
    }

# 把读取的数据存储到ES数据库中
storage_jkdata2es(each_item)
