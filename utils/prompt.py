import datetime
from typing import Union, List, Dict
def relevant_prompt(query , messages=""):

    messages = []
    query = "年发电量"
    messages = ["日发电量", "月发电量", "周发电量", "发电量", "2025年发电量"]
    messages_with_index = []

    for i, v in enumerate(messages):
        v = str(i) + "、" + v
        messages_with_index.append(v)

    prompt = f""" 
        ### 角色
        你是一个意图识别专家，接下来你需要分析用户查询问题与数据库信息，并识别出哪一条数据库信息与用户的查询问题最相关。
  
        ### 工作步骤
        1、首先，你要仔细分析用户查询问题和数据库信息；
        1、然后根据用户查询问题和数据库信息，识别出用户真正想要查询的数据库信息。
        2、最后指的该数据库信息对应的编号。

        ### 要求和限制
        1、只给出一个信息对应的数字编号；
        2、禁止直接输出信息的内容；
        
        现在请你根据下面的用户查询问题和数据库信息进行意图识别：
        **用户查询问题**: "{query}";
        **数据库信息**: "{messages_with_index}"
    """
    return prompt