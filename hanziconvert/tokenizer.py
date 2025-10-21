from hanlp_restful import HanLPClient
HanLP = HanLPClient('https://www.hanlp.com/api', auth='OTI2N0BiYnMuaGFubHAuY29tOlFpaWJqRmZlNkVXaWxJRnE=')
doc = HanLP.parse('2021年HanLPv2.1为生产环境带来次世代最先进的多语种NLP技术。阿婆主来到北京立方庭参观自然语义科技公司。')
print("doc:", doc)
similar_score = HanLP.semantic_textual_similarity([('年发电量', '日发电量'), ('年发电量', '月发电量'), ('年发电量', '2025年发电量')])
print("doc:", similar_score)