import os
import json

def getTypeMap():
    with open(os.path.join(os.path.dirname(os.getcwd()), 'conf','gbase2odpsMapTpye.json')) as f:
        typeMap = json.load(f)
    return typeMap