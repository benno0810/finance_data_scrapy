import pymongo
import time


class DB():
    def __init__(self,db_type='MongoDB',db_address='mongodb://localhost:27017/',db_name='db_test',table_name='col_test'):
        self.db_address=db_address
        self.db_type=db_type
        self.db_name=db_name
        self.table_name=table_name

    def connect(self):
        pass

    def insert_one(self):
        pass

    def delete_one(self):
        pass

    def test_connection(self):
        pass


class ProxyPool_DB(DB):
    def __init__(self,db_type='MongoDB',db_address='mongodb://localhost:27017/',db_name='proxy_pool',table_name='proxy_col'):
        super().__init__(db_type,db_address,db_name,table_name)
        self.client = pymongo.MongoClient(self.db_address)
        self.db=self.client[self.db_name]
        col=self.db[self.table_name]
        collist=self.db.list_collection_names()
        if self.table_name in collist:
            print('集合已存在')
        else:               
            line={
                'ip_address':'127.0.0.1:30300',
                'expires_time': time.time()
            }
            x=col.insert_one(line)
            print(x)
    

    def test_connection(self):        
        return True
                

    def insert_one(self,line:dict):
        super().insert_one()
        col=self.db[self.table_name]
        if self.test_connection() and line.get('ip_address'):
            if not line.get('expires_time'):
                #若没有过期时间戳则设置过期时间戳为180秒+
                line['expires_time']=time.time()+180
            col=self.db[self.table_name]
            x=col.insert_one(line)
            print(x)

    def delete_many(self,myquery:dict):
        col=self.db[self.table_name]
        x = col.delete_many(myquery)
        print(x.deleted_count, "个文档已删除")


    def delete_one(self,myquery:dict):
        super().delete_one()
    
    def find_many(self,myquery:dict):
        col=self.db[self.table_name]
        x=col.find(myquery)
        return x




if __name__=='__main__':
    db_test = ProxyPool_DB()
    line_test={
        'ip_address':'127.0.0.1:30031',
        'expires_time':time.time()-100
        }
    #db_test.insert_one(line_test)

    myquery={
        'ip_address':'127.0.0.1:30031'
    }
    myquery2={}
    x=list(db_test.find_many(myquery2))
    print(x)


        
            



    


