
def recursion_row_chidren_all(item, name_list):
    if len(item["children"]) != 0:
        name_list.append(item["value"])
        for c in item["children"]:
            recursion_row_chidren_all(c, name_list)
    else:
        name_list.append(item["value"])