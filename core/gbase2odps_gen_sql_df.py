#!/usr/bin/python
# -*- coding: UTF-8 -*-

import time
import re
import os
import json
import sys
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0,BASE_DIR)
import logging
import configparser
import pymysql
from tqdm import tqdm
import pandas as pd
from utilities import logFun,readConf,tableMsg,typeMap

logger = logFun.logModul('Gbase2Odps','gbase.log')
conf = readConf.readConf()



class ConnectGbase:
    def __init__(self):
        self.__conn = ConnectGbase.get_connect()

        if self.__conn:
            self.__cursor = self.__conn.cursor()

    @classmethod
    def get_connect(cls):
        try:
            conn = pymysql.connect(user=conf['gbase']['username'], \
                                   password=conf['gbase']['password'],\
                                   host=conf['gbase']['url'], \
                                   port=int(conf['gbase']['port']) )
        except Exception as e:
            logger.error('连接gbase出错,请检查gbase配置信息是否完善: %s' % e)
            conn = None
        return conn

    def execute_fetchone(self, sql):
        """执行指定sql语句"""
        try:
            self.__cursor.execute(sql)  # 执行语句
            res = self.__cursor.fetchone()
        except Exception as e:
            logger.warn(e)
        return res

    def execute_fetchall(self, sql):
        """执行指定sql语句"""
        try:
            self.__cursor.execute(sql)  # 执行语句
            res = self.__cursor.fetchall()
        except Exception as e:
            logger.warn(e)
        return res

    def close(self):
        """释放连接池资源"""
        self.__cursor.close()
        self.__conn.close()


class GbaseModels(ConnectGbase):
    def __init__(self,filename):
        super().__init__()
        self.tables = GbaseModels.readTable()
        self.odps_project_name = conf['odps']['odps_project_name'].strip()
        self.typeMap = typeMap.getTypeMap()
        self.time_str = time.strftime('%Y_%m_%d_%H_%M_%S', time.localtime())
        self.file_name = "{filename}_{time_str}.sql" .format(filename=filename,\
                                                             time_str=self.time_str)
        self.create_sql = ""
        self.create_tb_sql = ""
        self.count = 0
        self.f = open(os.path.join(os.path.dirname(os.getcwd()), 'modles', self.file_name), 'a+', encoding='utf-8')
    @classmethod
    def readTable(cls,name=conf['gbase']['file_name'].strip()):
        try:
            tables=tableMsg.getGbaseTablesNameObj(name)
        except Exception as e:
            logger.error('excel表信息配置错误，请按照gabse读取表信息模板配置')
            tables = None
        return tables



    def get_tables_info(self, dbs_tables=None):
        """
        查看当前登录的用户的表:用户名必须是大写
        :param cursor:
        :return:
        """
        # TEST为用户名，用户名必须是大写。
        # select * from all_tables where owner = 'TEST'
        # 查看当前登录的用户的表:用户名必须是大写
        # select table_name from user_tables;

        tb_names_gbase = []
        for dbTable in dbs_tables:
            sql = "select t.table_schema,t.table_name,t.table_comment from  information_schema.tables t \
            where t.table_schema = '%s' and t.table_name = '%s';" % (dbTable[0], dbTable[1])
            res = self.execute_fetchone(sql)
            if res:
                tb_names_gbase.append(res)
            else:
                logger.warning("数据库中%s表名%s的表不存在" % (dbTable[0], dbTable[1]))
        tb_name_list = []
        for index, tb_name in enumerate(tb_names_gbase):
            if isinstance(tb_name, tuple):
                db_name_str = tb_name[0]
                tb_name_str = tb_name[1]
                tb_comment_str = tb_name[2]
            else:
                db_name_str = tb_name['table_schema']
                tb_name_str = tb_name['table_name']
                tb_comment_str = tb_name['table_comment']
            if re.search('\W', str(tb_name_str)) or tb_name_str == 'nan' or \
                    tb_name_str == "":
                try:
                    logger.warning("数据库%s表名%s或业务领域名包含特殊字符" % (db_name_str, tb_name_str))
                    continue
                except ValueError as e:
                    continue
            if tb_comment_str is None:
                tb_comment_str = ""
            tb_info = {"db_name": db_name_str, "tb_name": tb_name_str, "tb_comment": tb_comment_str}
            tb_name_list.append(tb_info)
        return tb_name_list

    def col_type_oracle_odps(self,col_type):
        col_type = re.match(r'\w+', col_type).group()
        odpsColType = self.typeMap.get(col_type)
        if odpsColType:
            return odpsColType.upper()
        else:
            return odpsColType

    def get_table_ddl(self,db_name, tb_name, timeStr=None, typeMap=None):
        """
        根据数据库、表名过滤字段名，內型、评论等
        :param tb_name:
        :param timeStr:
        :param typeMap:
        :return:
        """
        sql = """select t.column_name,t.column_type,t.column_comment,t.character_maximum_length
                    from information_schema.columns t where t.table_schema = '%s' and t.table_name = '%s';
                    """ % (db_name, tb_name)
        result=self.execute_fetchall(sql)
        cols_info = []
        for row in result:
            col_name = row[0]
            if re.search('\W', str(col_name)) or col_name is None:
                logger.warning('%s表%s字段名含有特殊字符' % (tb_name, col_name))
                cols_info =[]
                return cols_info

            col_type = self.col_type_oracle_odps(row[1])
            if col_type is None:
                logger.warning("数据库%s-表名%s-字段%s包含有不能匹配字段类型%s" % (db_name, tb_name, col_name, row[1]))
                cols_info = []
                return cols_info
            col_comment = row[2]
            if col_comment is None:
                col_comment = ""
            col_m = {"col_name": "`%s`" % col_name, "col_type": col_type, "col_comment": col_comment, "db_name": db_name}
            cols_info.append(col_m)
        return cols_info

    @staticmethod
    def build_col(col_info):
        """
        拼接字段信息
        :param col_info:
        :return:
        """
        return '''{field_en} {field_type} COMMENT "{field_zh}",'''.format(field_en=col_info["col_name"],
                                                                          field_type=col_info["col_type"],
                                                                          field_zh=col_info["col_comment"])

    def build_odps_create_tb_sql(self,odps_project_name, tb_name, comment, cols):
        """
        转化成对应的odps建表语句
        :param odps_project_name:
        :param tb_name:
        :param comment:
        :param cols:
        :return:
        """
        col_list = []
        for col_info in cols:
            col_sql = self.build_col(col_info)
            col_list.append(col_sql)
        # extend_cols_str = "`ROWID` STRING COMMENT '扩展字段rowid'," + '\n\t'
        cols_all_str = '\n\t'.join(col_list)
        # cols_all_str = extend_cols_str + cols_str
        # cols_all_str = extend_cols_str + cols_str
        cols_all_str = cols_all_str[:-1]
        create_tb_sql = """CREATE TABLE IF NOT EXISTS {odps_project_name}.{tb_name} (
        {cols_str})
    COMMENT "{comment}"
    PARTITIONED BY (ds string)
    LIFECYCLE 5;
    """.format(odps_project_name=odps_project_name, tb_name=tb_name, cols_str=cols_all_str, comment=comment)

        return create_tb_sql.upper()

    def run(self):

        if self.tables:
            try:
                tb_infos = self.get_tables_info(dbs_tables=self.tables)
                #处理每张表的字段信息
                logger.info("开始处理查询到表字段信息......")
                for tb_info in tb_infos:
                    tb_name = tb_info["tb_name"]
                    db_name = tb_info["db_name"]
                    try:
                        cols_info = self.get_table_ddl(db_name, tb_name, self.time_str, self.typeMap)
                        tb_info["cols"] = cols_info
                    except Exception as e:
                        message = "%s数据库-%s表为创建成功:%s"%(db_name,tb_name,e)
                        logger.error(message)

                logger.info("开始转换成odps建表语句......")


                for tb_info in tqdm(tb_infos):
                    try:
                        if tb_info["cols"]:
                            tb_name =tb_info["tb_name"]
                            self.create_sql = self.build_odps_create_tb_sql(conf['odps']['odps_project_name'], tb_name, tb_info["tb_comment"],
                                                                  tb_info["cols"])
                            self.create_tb_sql += "\n" + self.create_sql
                            if self.count == 0:
                                create_tb_sql = "setproject odps.sql.type.system.odps2=true;" + "\n\n" + self.create_tb_sql
                                self.count += 1
                            else:
                                self.count += 1
                            self.f.write(self.create_tb_sql)
                            logger.info("Gbase to MaxCompute 创建第【%s】条表语句【%s】已经成功,总计%s条" % (self.count, tb_info["tb_name"],len(self.tables)))
                            self.create_sql = ""
                            self.create_tb_sql = ""
                        else:
                            continue
                    except Exception as e:
                        logger.error('该表%s创建失败:%s'%(tb_info['db_name'] + "-" + tb_info["tb_name"],e))
            finally:
                self.close()
                self.f.close()


if __name__ == '__main__':
    gm = GbaseModels('gabse2odps')
    gm.run()


# def conn_gbase(username, password, url, port):
#     """
#     创建gbase连接
#     :param username:
#     :param password:
#     :param url:
#     :return:
#     """
#     # 第一个参数是你的的登录oracle时的用户名
#     try:
#         conn = pymysql.connect(user=username, password=password, host=url, port=port)
#         cursor = conn.cursor()
#         return conn, cursor
#     except Exception as e:
#         logger.error('连接gbase出错,请检查gbase配置信息是否完善: %s'%e)



# def write_error_table(table_names=None, timeStr=None, comment=None):
#     with open(os.path.join(os.getcwd(), "no_processes_tables" + timeStr + '.txt'), 'a+') as f:
#         if isinstance(table_names, list):
#             f.write("\n".join(map(lambda x: x + "  ：" + comment, table_names)) + "\n")
#         elif isinstance(table_names, str):
#             f.write(table_names + "  ：" + comment + "\n", )
#         else:
#             f.write(table_names + "\n")


# def get_tables_info(cursor, dbs_tables=None, timeStr=None):
#     """
#     查看当前登录的用户的表:用户名必须是大写
#     :param cursor:
#     :return:
#     """
#     # TEST为用户名，用户名必须是大写。
#     # select * from all_tables where owner = 'TEST'
#     # 查看当前登录的用户的表:用户名必须是大写
#     # select table_name from user_tables;
#
#
#     tb_names_gbase = []
#     for dbTable in dbs_tables:
#         sql = "select t.table_schema,t.table_name,t.table_comment from  information_schema.tables t \
#         where t.table_schema = '%s' and t.table_name = '%s';" % (dbTable[0], dbTable[1])
#         res = cursor.execute(sql)
#         if res:
#             tb_names_gbase.append(cursor.fetchone())
#         else:
#             write_error_table("%s-%s" % (dbTable[0], dbTable[1]), timeStr=time_str, \
#                               comment="数据库中%s表名%s的表不存在" % (dbTable[0], dbTable[1]))
#             logger.warning("数据库中%s表名%s的表不存在" % (dbTable[0], dbTable[1]))
#
#
#     tb_name_list = []
#     for index,tb_name in enumerate(tb_names_gbase):
#         if isinstance(tb_name, tuple):
#             db_name_str = tb_name[0]
#             tb_name_str = tb_name[1]
#             tb_comment_str = tb_name[2]
#         else:
#             db_name_str = tb_name['table_schema']
#             tb_name_str = tb_name['table_name']
#             tb_comment_str = tb_name['table_comment']
#         if re.search('\W', str(tb_name_str)) or tb_name_str =='nan' or \
#                 tb_name_str == "" :
#             try:
#                 logger.warning("数据库%s表名%s或业务领域名包含特殊字符" % (db_name_str, tb_name_str))
#                 write_error_table(table_names=db_name_str + "-" + tb_name_str, timeStr=timeStr, comment='表名包含特殊字符')
#                 continue
#             except ValueError as e:
#                 continue
#         if tb_comment_str is None:
#             tb_comment_str = ""
#         tb_info = {"db_name": db_name_str, "tb_name": tb_name_str, "tb_comment":tb_comment_str}
#         tb_name_list.append(tb_info)
#     return tb_name_list
#
#
# def getTablesNameObj(fileName):
#     """
#     读取excel中的表名
#     :param file_name: 配置文件路径，可以自己指定
#     :param key:配置参数名字
#     :param args:
#     :param kwargs:
#     :return:
#     """
#     df = pd.read_excel(os.path.join(os.getcwd(), fileName)).drop_duplicates()
#     dbNames = map(lambda x: str(x).strip(), df['dbName'])
#     tableNames = map(lambda x: str(x).strip(), df['tableName'])
#     dbs_and_tables = zip(dbNames, tableNames)
#     return list(dbs_and_tables)
#
#
# def getTypeMap():
#     with open(os.path.join(os.getcwd(), 'gbase2odpsMapTpye.json')) as f:
#         typeMap = json.load(f)
#     return typeMap
#
#
# def close(conn, cursor):
#     """
#     关闭连接
#     :param conn:
#     :param cursor:
#     :return:
#     """
#     cursor.close()  # 关闭游标
#     conn.close()  # 关闭数据链接
#
#
# def col_type_oracle_odps(col_type, col_length=None, typeMap=None):
#     col_type = re.match(r'\w+',col_type).group()
#     odpsColType = typeMap.get(col_type)
#     if odpsColType:
#         return odpsColType.upper()
#     else:
#         return odpsColType
#

# def get_table_ddl(db_name, tb_name, timeStr=None, typeMap=None):
#     sql = """select t.column_name,t.column_type,t.column_comment,t.character_maximum_length
#                 from information_schema.columns t where t.table_schema = '%s' and t.table_name = '%s';
#                 """ % (db_name, tb_name)
#     cursor.execute(sql)
#     result = cursor.fetchall()
#     cols_info = []
#     for row in result:
#         col_name = row[0]
#         if re.search('\W', str(col_name)) or col_name is None:
#             logger.warning('%s表%s字段名含有特殊字符' % (tb_name, col_name))
#             write_error_table(table_names=db_name + "-" + tb_name, timeStr=timeStr, comment='字段名含有特殊字符')
#             cols_info =[]
#             return cols_info
#
#         col_type = col_type_oracle_odps(row[1], row[3], typeMap)
#         if col_type is None:
#             logger.warning("数据库%s-表名%s-字段%s包含有不能匹配字段类型%s" % (db_name, tb_name, col_name, row[1]))
#             write_error_table(table_names=db_name + "-" + tb_name, timeStr=timeStr, comment='表名包含特殊字符')
#             cols_info = []
#             return cols_info
#         col_comment = row[2]
#         if col_comment is None:
#             col_comment = ""
#         col_m = {"col_name": "`%s`" % col_name, "col_type": col_type, "col_comment": col_comment, "db_name": db_name}
#         cols_info.append(col_m)
#     return cols_info


# def build_col(col_info):
#     """
#     拼接字段信息
#     :param col_info:
#     :return:
#     """
#     return '''{field_en} {field_type} COMMENT "{field_zh}",'''.format(field_en=col_info["col_name"],
#                                                                       field_type=col_info["col_type"],
#                                                                       field_zh=col_info["col_comment"])


# def build_odps_create_tb_sql(odps_project_name, tb_name, comment, cols):
#     """
#     转化成对应的odps建表语句
#     :param odps_project_name:
#     :param tb_name:
#     :param comment:
#     :param cols:
#     :return:
#     """
#     col_list = []
#     for col_info in cols:
#         col_sql = build_col(col_info)
#         col_list.append(col_sql)
#     # extend_cols_str = "`ROWID` STRING COMMENT '扩展字段rowid'," + '\n\t'
#     cols_all_str = '\n\t'.join(col_list)
#     # cols_all_str = extend_cols_str + cols_str
#     # cols_all_str = extend_cols_str + cols_str
#     cols_all_str = cols_all_str[:-1]
#     create_tb_sql = """CREATE TABLE IF NOT EXISTS {odps_project_name}.{tb_name} (
#     {cols_str})
# COMMENT "{comment}"
# PARTITIONED BY (ds string)
# LIFECYCLE 5;
# """.format(odps_project_name=odps_project_name, tb_name=tb_name, cols_str=cols_all_str, comment=comment)
#
#     return create_tb_sql.upper()


# if __name__ == '__main__':
# #     is_read_database_all_tables = input("是否需要读gbase某一个数据库全部表信息,\
# # True代表是，False代表读取的指定表信息>>>").strip()
#     conf = readConf()
#     file_name = conf['default']['file_name'].strip()
#     input_username = conf['gbase']['username']
#     input_password = conf['gbase']['password']
#     input_url = conf['gbase']['url']
#     input_port = int(conf['gbase']['port'])
#     odps_project_name = conf['odps']['odps_project_name']
#
#     time_str = time.strftime('%Y_%m_%d_%H_%M_%S', time.localtime())
#     if input_username is None or input_username == "":
#         logger.info("请输gbase入账号......")
#         time.sleep(5)
#         sys.exit(0)
#     if input_password is None or input_password == "":
#         logger.info("请输入gbase密码......")
#         time.sleep(5)
#         sys.exit(0)
#     if input_url is None or input_url == "":
#         logger.info("请输入gbase链接地址......")
#         time.sleep(5)
#         sys.exit(0)
#     # if is_read_database_all_tables is None or is_read_database_all_tables  not in ['True','False']:
#     #     logger.info("请输入正确判断信息......")
#     #     time.sleep(5)
#     #     sys.exit(0)
#     message = "您输入的账号为：%s，密码为:%s，链接地址为：%s" % (input_username, input_password, input_url)
#     logger.info(message)
#     conn, cursor = conn_gbase(input_username, input_password, input_url, input_port)
#     logger.info("连接oracle数据库成功")
#     # 获取所有的表信息
#     # if is_read_database_all_tables == 'True':
#     #     tb_infos = get_tables_info(cursor, timeStr=time_str)
#     # else:
#     logger.info("开始加载指定表名......")
#     try:
#         tableNames =  (file_name)
#         tb_infos = get_tables_info(cursor, dbs_tables=tableNames, timeStr=time_str)
#         total = len(tableNames)
#     except Exception as e:
#         logger.error(e)
#         time.sleep(20)
#         sys.exit(0)
#
#     # 处理每张表的字段信息
#     logger.info("开始处理查询到表字段信息......")
#     typeMap = getTypeMap()
#     cols_info=[]
#     for tb_info in tb_infos:
#         tb_name = tb_info["tb_name"]
#         db_name = tb_info["db_name"]
#         try:
#             cols_info = get_table_ddl(db_name, tb_name, time_str, typeMap)
#             tb_info["cols"] = cols_info
#         except Exception as e:
#             message = "%s数据库-%s表为创建成功:%s"%(db_name,tb_name,e)
#             logger.error(message)
#             write_error_table(table_names=db_name+"-"+"tb_name",timeStr=time_str,comment=message)
#
#     logger.info("开始转换成odps建表语句......")
#     file_name = "gbase2odps_tables_%s.sql" % time_str
#     f = open(os.path.join(os.getcwd(), file_name), 'a+', encoding='utf-8')
#     create_sql = ""
#     create_tb_sql = ""
#     count = 0
#
#     for tb_info in tqdm(tb_infos):
#         try:
#             if tb_info["cols"]:
#                 tb_name =tb_info["tb_name"]
#                 create_sql = build_odps_create_tb_sql(odps_project_name, tb_name, tb_info["tb_comment"],
#                                                       tb_info["cols"])
#                 create_tb_sql += "\n" + create_sql
#                 if count == 0:
#                     create_tb_sql = "setproject odps.sql.type.system.odps2=true;" + "\n\n" + create_tb_sql
#                     count += 1
#                 else:
#                     count += 1
#                 f.write(create_tb_sql)
#                 logger.info("Gbase to MaxCompute 创建第【%s】条表语句【%s】已经成功,总计%s条" % (count, tb_info["tb_name"],total))
#                 create_sql = ""
#                 create_tb_sql = ""
#             else:
#                 continue
#         except Exception as e:
#             logger.error(e)
#             write_error_table(table_names=tb_info['db_name'] + "-" + tb_info["tb_name"], timeStr=time_str,
#                               comment='该表创建失败:%s'%e)
#     f.close()
#     close(conn, cursor)
#     time.sleep(15)
