import json
import pandas as pd

def transform_data(json_data):
    """
    将 JSON 数据转换为 Pandas DataFrame。

    :param json_data: JSON 数据
    :return: 转换后的 Pandas DataFrame
    """
    datas = json.load(json_data)
    df_resultSets = pd.json_normalize(datas['resultSets'])
    row_datas=[]
    for i in range(len(df_resultSets)):
        for j in range(len(df_resultSets.iloc[i,2])):
            row_datas.append(df_resultSets.iloc[i,2][j])

    headers = df_resultSets.iloc[0,1]
    df = pd.DataFrame(row_datas,columns=headers)

    return df
