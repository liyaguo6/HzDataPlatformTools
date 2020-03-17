import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0,BASE_DIR)

from utilities import logFun,readConf,tableMsg

logger=logFun.logModul('GBASE','gbase.log')

logger.warn('警告！！')

conf = readConf.readConf()
print(conf['default']['file_name'])

tables = tableMsg.getGbaseTablesNameObj('gbase2odps_table_df.xlsx')

print(tables)
