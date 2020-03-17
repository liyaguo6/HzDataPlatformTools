import os
import pandas as pd

def getGbaseTablesNameObj(fileName):
    """
    读取excel中的表名
    :param file_name: 配置文件路径，可以自己指定
    :param key:配置参数名字
    :param args:
    :param kwargs:
    :return:
    """
    df = pd.read_excel(os.path.join(os.path.dirname(os.getcwd()),'data', fileName)).drop_duplicates()
    dbNames = map(lambda x: str(x).strip(), df['dbName'])
    tableNames = map(lambda x: str(x).strip(), df['tableName'])
    dbs_and_tables = zip(dbNames, tableNames)
    return list(dbs_and_tables)