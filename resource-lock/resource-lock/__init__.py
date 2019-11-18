import os
import time
import random
import socket
import traceback

from portalocker.exceptions import LockException
from resource_lock.exceptions import UserQueueError
from resource_lock.controller import Queue, Resource, LOG
from resource_lock.models import DbBase

RETRY_ENABLED = True
RETRY_TIMES = 30
RETRY_INTERVAL_MAX = 10

NETWORK_ERROR = (socket.error, EOFError, LockException, UserQueueError)


def retry(fn):
    def _retry(*args, **kwargs):
        retry_times = RETRY_TIMES if RETRY_ENABLED else 1
        for i in range(retry_times):
            try:
                return fn(*args, **kwargs)
            except NETWORK_ERROR as e:
                if i < retry_times - 1:
                    LOG.warning("%s failed(%s): %s\n%s" %
                                (fn.func_name, type(e), e, traceback.format_exc()))
                    time.sleep(random.randint(1, RETRY_INTERVAL_MAX))
                else:
                    raise

    return _retry

class Resources(object):
    BASE_DIR = "D:/" if os.name == "nt" else "/tmp/"
    Queue.LOCAL_QUEUE_LOCK_PATH = "%sresource_controller_queue_lock" % BASE_DIR
    Resource.LOCAL_RESOURCE_LOCK_PATH = "%sresource_controller_resource_lock" % BASE_DIR
    DbBase.MONGODB_HOST = "10.134.43.100"
    DbBase.MONGODB_PORT = 27017

    @classmethod
    def set_conf(cls, queue_lock_path=None, resource_lock_path=None,
                 mongodb_host=None, mongodb_port=None):
        if queue_lock_path:
            Queue.LOCAL_QUEUE_LOCK_PATH = queue_lock_path
        if resource_lock_path:
            Resource.LOCAL_RESOURCE_LOCK_PATH = resource_lock_path
        if mongodb_host:
            DbBase.MONGODB_HOST = mongodb_host
        if mongodb_port:
            DbBase.MONGODB_PORT = mongodb_port

    @classmethod
    @retry
    def initialize(cls, schema_list, schema_for_schema=None, clean=False):
        """
        see Resource.initialize
        """
        return Resource.initialize(schema_list=schema_list,
                                   schema_for_schema=schema_for_schema, clean=clean)

    @classmethod
    @retry
    def register(cls, category, descriptor, status='idle', mode=None,
                 user=None, user_limit=0, _id=None, for_ci=False, check=True):
        """
        see Resource.register
        """
        return Resource.register(category=category, descriptor=descriptor,
                                 status=status, mode=mode, user=user,
                                 user_limit=user_limit, _id=_id, for_ci=for_ci, check=check)
	
	@classmethod
    @retry
    def unregister(cls, _id):
        """
        see Resource.unregister
        """
        return Resource.unregister(_id=_id)

    @classmethod
    @retry
    def update(cls, _id, update, force=False):
        """
        see Resource.update
        """
        return Resource.update(_id=_id, update=update, force=force)

    @classmethod
    @retry
    def list(cls, query_filter=None):
        """
        see Resource.list
        """
        return list(Resource.list(query_filter=query_filter))

    @classmethod
    @retry
    def filter(cls, resource_filter, user,
               user_own=False, except_resource=None, rule_map=None):
        """
        see Resource.filter
        """
        return Resource.filter(
            resource_filter=resource_filter, user=user,
            user_own=user_own, except_resource=except_resource, rule_map=rule_map)
			
			
	@classmethod
    @retry
    def lock(cls, resource_filter, user, merge=True, except_resource=None, rule_map=None):
        """
        see Resource.lock
        """
        return Resource.lock(resource_filter=resource_filter, user=user,
                             merge=merge, except_resource=except_resource, rule_map=rule_map)

    @classmethod
    @retry
    def unlock(cls, user, filter=None, resource_list=None, resource_dict=None):
        """
        see Resource.unlock
        """
        return Resource.unlock(user=user, filter=filter,
                               resource_list=resource_list,
                               resource_dict=resource_dict)

    @classmethod
    @retry
    def public_to_private(cls, _id, user):
        """
        see Resource.public_to_private
        """
        return Resource.public_to_private(_id=_id, user=user)

    @classmethod
    @retry
    def add_mutex(cls, _id, user, mutex):
        """
        see Resource.add_mutex
        """
        return Resource.add_mutex(_id=_id, user=user, mutex=mutex)

    @classmethod
    @retry
    def remove_mutex(cls, _id, user, mutex):
        """
        see Resource.remove_mutex
        """
        return Resource.remove_mutex(_id=_id, user=user, mutex=mutex)

    @classmethod
    @retry
    def clean_user(cls, _id):
        """
        see Resource.clean_user
        """
        return Resource.clean_user(_id=_id)

    @classmethod
    def change_db(cls, db):
        """
        change the mongodb database of resource, schema and queue

        :param db: database name
        :return:
        """
        DbBase.DB_NAME = db

    @classmethod
    def change_collection(cls, collection):
        """
        change the mongodb collection of resource

        :param collection: resource collection name
        :return:
        """
        DbBase.RESOURCE_NAME = collection
		
		
	@classmethod
    def change_schema_collection(cls, collection):
        """
        change the mongodb collection of resource schema

        :param collection: resource schema collection name
        :return:
        """
        DbBase.SCHEMA_NAME = collection

    @classmethod
    def change_queue_collection(cls, collection):
        """
        change the mongodb collection of resource schema

        :param collection: queue collection name
        :return:
        """
        DbBase.QUEUE_NAME = collection

    @classmethod
    def clean_mongo_client(cls):
        """
        clean all mongo client, so there is not client cache

        :return:
        """
        for client_dict in DbBase.CLIENT_CACHE.itervalues():
            for client in client_dict.itervalues():
                client.close()
        DbBase.CLIENT_CACHE.clear()