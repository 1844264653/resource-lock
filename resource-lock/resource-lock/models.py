from pymongo import MongoClient


class DbBase(object):
    DB_NAME = "resource_controller"
    RESOURCE_NAME = "resource"
    SCHEMA_NAME = "resource_schema"
    QUEUE_NAME = "queue"
    CLIENT_CACHE = {}

    def __init__(self, host=None, port=27017, password=None):
        if host is None:
            raise ValueError("The mongodb host is not set")

        self.client = self.init_client(host, port)

    def init_client(self, host, port):
        if host in self.__class__.CLIENT_CACHE:
            client = self.__class__.CLIENT_CACHE[host].get(port)
            if client:
                return client

        client = MongoClient(host, int(port))
        if host not in self.__class__.CLIENT_CACHE:
            self.__class__.CLIENT_CACHE[host] = {}
        self.__class__.CLIENT_CACHE[host][port] = client
        return client


class Resource(DbBase):
    def __init__(self):
        super(Resource, self).__init__(host=self.__class__.MONGODB_HOST,
                                       port=self.__class__.MONGODB_PORT)
        self.db = self.client[self.__class__.DB_NAME]
        self.collection = self.db[self.__class__.RESOURCE_NAME]


class Resource_Schema(DbBase):
    def __init__(self):
        super(Resource_Schema, self).__init__(host=self.__class__.MONGODB_HOST,
                                              port=self.__class__.MONGODB_PORT)
        self.db = self.client[self.__class__.DB_NAME]
        self.collection = self.db[self.__class__.SCHEMA_NAME]


class Queue(DbBase):
    def __init__(self):
        super(Queue, self).__init__(host=self.__class__.MONGODB_HOST,
                                    port=self.__class__.MONGODB_PORT)
        self.db = self.client[self.__class__.DB_NAME]
        self.collection = self.db[self.__class__.QUEUE_NAME]