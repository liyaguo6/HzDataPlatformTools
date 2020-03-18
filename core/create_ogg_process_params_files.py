#!/usr/bin/python
# -*- coding: UTF-8 -*-
#Author:Liyaguo

import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0,BASE_DIR)
import pandas as pd
from utilities import logFun,readConf,tableMsg

logger = logFun.logModul('OggProcessFile','OggProcessFile.log')
conf = readConf.readConf()

df = tableMsg.getOggProcessMsg('ogg_process_params.xlsx')
processNameList=list(df['processName'].unique())

def makeProcessParams(processNameList,df):
    for processName in processNameList:
        if pd.notnull(processName):
            user=df[df['processName']==processName]['user'].mode().values[0]
            tables=df[df['processName']==processName]['tableName'].values
            yield processName,user,list(tables)


class CreateProcessParamsFile:
    def __init__(self):
        self.datahub_ogg_plugin_home = conf['ogg']['datahub_ogg_plugin_home']
        self.ogg_home = conf['ogg']['ogg_home']
        self.dirdef_name = conf['ogg']['dirdef_name']
        self.dirdat_name = conf['ogg']['dirdat_name']
        self.ProcessParamsItreator = makeProcessParams(processNameList,df)


    def paramsFileModlues(self,processName,dirdat,path,dirdef,tables):
        processParamsStr ="""#编辑内容
#add extract {processName},exttrailsource {dirdat}
extract {processName}
getEnv (JAVA_HOME)
getEnv (LD_LIBRARY_PATH)
getEnv (PATH)
CUSEREXIT ./libggjava_ue.so CUSEREXIT PASSTHRU INCLUDEUPDATEBEFORES,PARAMS {path}
sourcedefs {dirdef}
{tables}""".format(processName=processName,dirdat=dirdat,path=path,dirdef=dirdef,tables=tables)
        return processParamsStr

    def makeTables(self,user,tableList):
        tablesStr=""
        for table in tableList:
            tmp = 'table %s.%s \n'%(user,table)
            tablesStr +=tmp
        return tablesStr[:-1]
    def create_rpm_file(self):
        for processName,user,tableList in self.ProcessParamsItreator:
            try:
                f=open(os.path.join(os.path.dirname(os.getcwd()),'modles/ogg/%s'%processName+'.rpm'),'w',encoding='utf -8')
                tablesStr=self.makeTables(user,tableList)
                dirdef = './dirdef/%s.def'%self.dirdef_name
                dirdat = './dirdat/%s'%self.dirdat_name
                path = "'%s/ggsconfig/%s/%s.properties'"%(self.datahub_ogg_plugin_home,processName,processName)
                paramsStr=self.paramsFileModlues(processName,dirdat,path,dirdef,tablesStr)
                f.write(paramsStr)
                f.close()
            except Exception as e:
                logger.error("该进程%s文件未创建成功,错误原因:%s,请检查ogg配置参数文件是否符合规范"%(processName,e))
                continue


if __name__ == '__main__':
    # print(list(makeProcessParams(processNameList,df)))
    cp=CreateProcessParamsFile()
    cp.create_rpm_file()