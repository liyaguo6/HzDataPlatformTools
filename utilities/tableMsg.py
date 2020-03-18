import os
import pandas as pd

def getGbaseTablesNameObj(fileName):
    """
    读取gbase的excel表中的信息
    :param file_name: 配置文件路径，可以自己指定
    :param key:配置参数名字
    :return:
    """
    df = pd.read_excel(os.path.join(os.path.dirname(os.getcwd()),'data', fileName)).drop_duplicates()
    dbNames = map(lambda x: str(x).strip(), df['dbName'])
    tableNames = map(lambda x: str(x).strip(), df['tableName'])
    dbs_and_tables = zip(dbNames, tableNames)
    return list(dbs_and_tables)



def getOggProcessMsg(fileName):
    """
    读取ogg中的进程配置信息
    :param filename:文件名
    :return:
    """
    df = pd.read_excel(os.path.join(os.path.dirname(os.getcwd()), 'data', fileName)).drop_duplicates()
    return df