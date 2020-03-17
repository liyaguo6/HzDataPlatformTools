import os
import configparser
def readConf():
    """
    :param file_name: 配置文件路径，可以自己指定
    :param key:配置参数名字
    :param args:
    :param kwargs:
    :return:
    """
    conf = configparser.ConfigParser()
    conf.read(os.path.join(os.path.dirname(os.getcwd()),'conf', 'conf.ini'), encoding='utf-8')
    return conf
