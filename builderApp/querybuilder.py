import json
from datetime import datetime


# {"type":"column","tableName":"products","sharedkey":["ProductId"],"primarykey":"ProductId","key":["Price","Color"],"columns":["ProductId INT","Color VARCHAR(10)","Price INT","dt DATETIME"]}
with open("/home/prasad/PycharmProjects/PythonDev/querybuilder.json", "r") as read_file:
    conf = json.load(read_file)

def list_string(li,type=True):
    if type:
        return "\n,".join(str(x) for x in li)
    else:
        return ",".join(str(x) for x in li)
# print(list_string(['ProductId INT', 'Color VARCHAR(10)', 'Price INT', 'dt DATETIME' ]))

def Dict2Str(dictin,joiner=','):
    # make dict to str, with the format key='value'
    #tmpstr=''
    tmplist=[]
    for k,v in dictin.items():
        # if v is list, so, generate 
        # "k in (v[0], v[1], ...)"
        if isinstance(v, (list, tuple)):
            tmp = str(k)+' in ('+ ','.join(map(lambda x:'\''+str(x)+'\'',v)) +') '
        else:
            tmp = str(k)+'='+'\''+str(v)+'\''
        tmplist.append(' '+tmp+' ')
    return joiner.join(tmplist)

def gen_update(table,dicts,conddicts):
    # conddicts maybe the Condition, in sql, where key='value' or key in (value)
    # dicts are the values to update
    sql = ''
    sql += 'update %s '%table
    sql += ' set %s'%Dict2Str(dicts)
    sql += ' where %s'%Dict2Str(conddicts,'and')
    return sql

def gen_insert(table,dicts):
    '''
    >>> kdict = {'name':'lin','age':22} 
    >>> geninsertsql('persons',kdict)
    insert into person (name,age) values ('lin',22)
    '''
    sql = 'insert into %s '%table
    ksql = []
    vsql = []
    for k,v in dicts.items():
        ksql.append(str(k))
        vsql.append('\''+str(v)+'\'')
    sql += ' ('+','.join(ksql)+') '
    sql += ' values ('+','.join(vsql)+')'
    return sql

def gen_select(table,keys="*",conddicts=None):
    if isinstance(keys, (tuple,list)):
        keys=','.join(map(str,keys))
    sql = 'select %s '%keys
    sql += ' from %s '%table
    if conddicts:
        sql += ' where %s '%Dict2Str(conddicts,'and')
    #print sql
    return sql

def gen_table_ddl():
    '''
    :definition: This creates table DDL for given JSON input
    :COLUMN STORE:
        1. Columnstore format is enabled by adding a key with the CLUSTERED COLUMNSTORE
        2.
    :return:
    '''
    dictin = conf
    sql = ''
    if dictin['type'] == 'row':
        sql += f'CREATE TABLE RS_{dictin["prefix"].upper()}_{dictin["tableName"].upper()} ( '
        sql += f'{list_string(dictin["columns"])} '
        sql += '\n,INS_TS timestamp DEFAULT CURRENT_TIMESTAMP'
        sql += '\n,UPD_TS timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'
        if isinstance(dictin['key'], list):
            for i in dictin['key']:
                sql += f'\n,KEY ({i})'
        else:
            sql += f'\n,KEY ({dictin["key"]})'
        sql += f'\n,PRIMARY KEY({dictin["primarykey"]}),'
    else:
        sql += f'CREATE TABLE CS_{dictin["prefix"].upper()}_{dictin["tableName"].upper()} ( '
        sql += f'{list_string(dictin["columns"])} '
        sql += '\n,INS_TS timestamp DEFAULT CURRENT_TIMESTAMP'
        sql += '\n,UPD_TS timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'
        if isinstance(dictin['key'], list):
                sql += f'\n,KEY ({list_string(dictin["key"],False)}) USING CLUSTERED COLUMNSTORE'
        else:
            sql += f'\n,KEY ({dictin["key"]}) USING CLUSTERED COLUMNSTORE'


    if isinstance(dictin['sharedkey'], list):
        for i in dictin['sharedkey']:
            sql += f'\n,SHARD KEY ({i})'
    else:
        sql += f'\n,SHARD KEY ({dictin["sharedkey"]})'
    sql += '\n);'
    return sql

def isvaliddatetime(y,m,d,h,minutes,s):
    try:
        x = datetime(y,m,d,h,minutes,s)
        return True
    except:
        return False
def gendatetime(*args):
    #y,m,d,h,minutes,s
    if not isvaliddatetime(*args):
        return None
    return '-'.join(map(str,args))

def gensql(imp,*args, **kwds):
    if imp == "insert":
        return gen_insert(*args, **kwds)
    elif imp == "update":
        return gen_update(*args, **kwds)
    elif imp == "select":
        return gen_select(*args, **kwds)
    else:
        return None



if __name__ == '__main__':
    # print (gensql("select",'NextIDs','ID',{'TableName':'RealRawReplicas'}))      # select
    # print gensql("insert",'NextIDs',{'TableName':'RealRecFiles','ID':'0'})     # insert
    # print gensql("update",'NextIDs',{'TableName':'RealRecFiles'},{'ID':'1'})   # update
    # print (Dict2Str({'TableName':'RealRecFiles','SthKey':'SthValue', 'keyslist':range(10)}, "and"))
    print(gen_table_ddl())
    # print gensql("select", 'mytable', [1,2], {"x":range(10)})