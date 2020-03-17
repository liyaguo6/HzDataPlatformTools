import os
import logging

def logModul(topicName,fileName):
    """
     ##设置日志处理
    :param topicName: 日志主题
    :param fileName: 日志存储文件
    :return:
    """
    logger = logging.getLogger(topicName)
    logging.basicConfig(level=logging.INFO)
    ch = logging.StreamHandler()
    fh = logging.FileHandler(os.path.join(os.path.dirname(os.getcwd()),'logs' ,fileName))
    fh.setLevel(logging.WARNING)
    logger.addHandler(ch)
    logger.addHandler(fh)
    file_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    console_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    ch.setFormatter(console_formatter)
    fh.setFormatter(file_formatter)
    return logger