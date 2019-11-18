import copy
import portalocker
import random
import time
import pymongo
import uuid
import jsonschema
import collections
import re
import itertools
import logging

from resource_lock import models
from resource_lock import exceptions as res_except

HANDLE = logging.StreamHandler()
HANDLE.setFormatter(logging.Formatter(
    '%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
LOG.addHandler(HANDLE)

# begin: monkey patch for jsonschema.validate
old_validate = jsonschema.validate


def validate(instance, schema, cls=None, *args, **kwargs):
    kwargs["format_checker"] = jsonschema.FormatChecker()
    return old_validate(instance, schema, cls, *args, **kwargs)


jsonschema.validate = validate


# end: monkey patch for jsonschema.validate


class Utils(object):
    @classmethod
    def random_str(cls):
        r = random.sample("abcdefghijklmnopqrstuvwxyz01234567890", 24)
        return "".join(r)

    @classmethod
    def now(cls):
        return time.time()


def lock_queue(fn):
    def wrapper(cls, *args, **kwargs):
        path = cls.LOCAL_QUEUE_LOCK_PATH
        with portalocker.Lock(path, timeout=30):
            ret = fn(cls, *args, **kwargs)
        return ret

    return wrapper


def lock_resource(fn):
    def wrapper(cls, *args, **kwargs):
        path = cls.LOCAL_RESOURCE_LOCK_PATH
        with portalocker.Lock(path, timeout=30):
            ret = fn(cls, *args, **kwargs)
        return ret

    return wrapper


def wait_for_queue(fn, timeout=180):
    def wrapper(cls, *args, **kwargs):
        user = kwargs["user"]
        old_first_stay_timeout = 60
        try:
            Queue.push(user=user)
            begin = Utils.now()
            old_first = None
            old_first_stay_begin = begin
            while True:
                index, first = Queue.index_of(user=user)
                if index is None:
                    LOG.warning("The user(%(user)s) suddenly disappears "
                                "from the queue, may be considered residual" % {"user": user})
                    Queue.push(user=user)
                else:
                    if index == 0:
                        break
                    elif (Utils.now() - begin) < timeout:
                        if old_first is None:
                            old_first = first
                        if Utils.now() - old_first_stay_begin >= old_first_stay_timeout:
                            if old_first == first:
                                LOG.warning("The user %s seems to be residual "
                                            "and will be deleted" % old_first)
                                Queue.pop(user=old_first["_id"])
                            else:
                                old_first = first
                                old_first_stay_begin = Utils.now()
                    else:
                        raise res_except.UserQueueError(
                            "Wait user(%(user)s) queue failed during %(time)s seconds: %(queue)s" %
                            {"user": user, "time": timeout, "queue": list(Queue.list())})

            ret = fn(cls, *args, **kwargs)

        finally:
            Queue.pop(user=user)
        return ret

    return wrapper


class Resource(object):
    def __init__(self, descriptor=None, mode=None, env=None):
        if descriptor is None:
            raise ValueError("The descriptor of resources should not be None")

        self.descriptor = descriptor
        self.mode = mode
        self.env = env

    @classmethod
    def get_resource_model(cls):
        model = models.Resource()
        return model.collection

    @classmethod
    def get_resource_schema_model(cls):
        model = models.Resource_Schema()
        return model.collection

    @classmethod
    def list(cls, query_filter=None, sort=pymongo.ASCENDING):
        """
        List the resources that meet the query filter

        :param query_filter: a dict to describe query filter rules(mongodb syntax)
        :param sort: the type to sort resources by create_time
        :return: the list of filtered resources with complete information
        """

        resource_model = cls.get_resource_model()

        if query_filter is None:
            query_filter = {}

        resources = resource_model.find(query_filter).sort("create_time", sort)
        return resources

    @classmethod
    def _lock(cls, resource_filter, resources, user, check=True):
        """
        Lock resources for user

        :param resource_filter: a dict in "resource_name: filter_rules" format
        :param resources: a dict of resources locked in "resource_name: resource_info" format
        :param user: resources locked by who
        :param check: True for check if the status of resource is abnormal after locking
        """
        resource_model = cls.get_resource_model()
        resources_locked = []
        try:
            for name, resource in resources.items():
                filter = resource_filter[name]
                mutex = filter.get("mutex") or []
                if mutex:
                    mutex = list(set(mutex))
                is_list = True
                if not isinstance(resource, list):
                    is_list = False
                    resource = [resource]

                old_resource = []
                new_resource = []
                for index, res in enumerate(resource):
                    if user in res["user"]:
                        # if it needs to convert public mode to private
                        if not filter.get("public") and res["mode"] == "public":
                            # public -> private
                            update_data = {
                                "status": "busy",
                                "mode": "private",
                                "mutex": {},
                                "update_time": Utils.now()
                            }
                            id_filter = {"_id": res["_id"]}
                            resource_model.update_one(filter=id_filter,
                                                      update={"$set": update_data})

                        elif res["mode"] == "public" and mutex != res["mutex"].get(user, []):
                            # public -> public
                            if mutex:
                                res["mutex"][user] = mutex
                            else:
                                res["mutex"].pop(user)

                            update_data = {
                                "mutex": res["mutex"],
                                "update_time": Utils.now()
                            }
                            id_filter = {"_id": res["_id"]}
                            resource_model.update_one(filter=id_filter,
                                                      update={"$set": update_data})
                        old_resource.append(res)
                        continue

                    user_list = list(res["user"])
                    user_list.append(user)
                    update_data = {
                        "status": "busy",
                        "mode": "private",
                        "user": user_list,
                        "update_time": Utils.now()
                    }
                    if mutex:
                        res["mutex"][user] = mutex
                        update_data["mutex"] = res["mutex"]

                    if filter.get("public") is True:
                        update_data["mode"] = "public"
                        user_limit = res["user_limit"]
                        if user_limit == 1:
                            raise ValueError("Please use private mode to lock "
                                             "the resource: %s" % resource)
                        if user_limit == 0 or (
                                (len(res["user"]) + 1) < user_limit):
                            update_data["status"] = "using"

                    id_filter = {"_id": res["_id"]}
                    resource_model.update_one(filter=id_filter,
                                              update={"$set": update_data})
                    new_res_info = resource_model.find_one(filter=id_filter)
                    new_resource.append(new_res_info)
                    resources_locked.append(new_res_info)
                    if check:
                        cls._check_resource_status(new_res_info)

            all_resource = old_resource + new_resource
            if len(all_resource) == 1 and not is_list:
                resources[name] = all_resource[0]
            else:
                resources[name] = all_resource

        except:
            for resource in resources_locked:
                cls._unlock(resource_model, resource, user)
            raise

    @classmethod
    @wait_for_queue
    @lock_resource
    def lock(cls, resource_filter, user, merge=True,
             except_resource=None, rule_map=None, quiet=True):
        """
        Lock resources in mongodb according to resource_filter


        :param resource_filter: a dict in "resource_name: filter_rules" format
        :param user: the name of user who will lock resource
        :param merge: True for merge res_1 and res_2 into res when return
        :param except_resource: a list of _id of resource to be remove from search scope
        :param rule_map: a dict in "abstract_rule: advanced_rule" format
        :param quiet: True for no log output
        :return: the dict of resources locked in "resource_name: resource_info" format
        """
        try:
            # check if the resources owned by user can satisfy the filter
            resources = cls.filter(resource_filter, user, user_own=True,
                                   except_resource=except_resource,
                                   rule_map=rule_map, quiet=quiet)
        except res_except.ResourceNotFoundError:
            try:
                resources = cls.filter(resource_filter, user,
                                       except_resource=except_resource,
                                       rule_map=rule_map, quiet=quiet)
            except res_except.ResourceNotFoundError:
                try:
                    cls.filter(resource_filter, user,
                               except_resource=except_resource,
                               rule_map=rule_map, quiet=quiet, ignore_status=True)
                except res_except.ResourceNotFoundError as e:
                    raise res_except.ResouceNotExistError(e)
                else:
                    raise

        cls._lock(resource_filter, resources, user)

        if merge:
            cls._merge_separated_resource(resources)
        return resources

    @classmethod
    def _unlock(cls, resource_model, resource, user, check=True, strict=True):
        """
        Unlock the resource locked by user


        :param resource_model: the mongodb object of resource collection
        :param resource: a dict of full info of resource or the _id of resource
        :param user: resources locked by who
        :param check: True for check if the status of resource is abnormal after unlocking
        :param strict: True for raise error if user not in resourece["user"]
        """
        if isinstance(resource, dict):
            resource_id = resource["_id"]
        else:
            resource_id = resource
        resource = resource_model.find_one(filter={"_id": resource_id})

        if user not in resource["user"]:
            if strict:
                raise res_except.UserNotInUsersWhenUnlock(
                    "Do not found user(%(user)s) in %(resource)s" %
                    {"user": user, "resource": resource})
            return False

        user_list = list(resource["user"])
        user_list.remove(user)
        if resource["mode"] == "private" or (len(resource["user"]) - 1 == 0):
            status = "idle"
            mode = None
        else:
            status = "using"
            mode = "public"

        update_data = {
            "status": status,
            "mode": mode,
            "user": user_list
        }
        if user in resource["mutex"]:
            resource["mutex"].pop(user)
            update_data["mutex"] = resource["mutex"]
        id_filter = {"_id": resource["_id"]}
        resource_model.update_one(filter=id_filter,
                                  update={"$set": update_data})
        if check:
            new_res_info = resource_model.find_one(filter=id_filter)
            cls._check_resource_status(new_res_info)
        return True

    @classmethod
    @wait_for_queue
    @lock_resource
    def unlock(cls, user, filter=None, resource_list=None, resource_dict=None):
        """
        Unlock resources locked by user in mongodb

        :param user: resources locked by who
        :param filter: a dict to describe resources filter rules(mongodb syntax) for resource
        :param resource_list: a list of _id or full info of resources
                              such as [{res_1}, {res_2}, ...] or [res_1_id, ...]
        :param resource_dict: a dict of resources returned by lock()
                              such as {{'res_1': {'_id': ...}, {'res_2': {...}}, ...}
        :return: the list of resources unlocked
        """
        if int(bool(filter)) + int(bool(resource_list)) + int(bool(resource_dict)) > 1:
            raise ValueError("Only one can be specified in filter/resource_list/resource_dict")
        if filter is None and resource_list is None and resource_dict is None:
            filter = {}

        unlocked_list = []
        resource_model = cls.get_resource_model()
        if filter is not None:
            for resource in resource_model.find(filter):
                if cls._unlock(resource_model, resource, user, strict=False):
                    unlocked_list.append(resource)
        else:
            if resource_dict is not None:
                resource_list = []
                for resource in resource_dict.values():
                    if isinstance(resource, list):
                        resource_list += resource
                    else:
                        resource_list.append(resource)

            for resource in resource_list:
                if cls._unlock(resource_model, resource, user):
                    unlocked_list.append(resource)

        return unlocked_list

    @classmethod
    @wait_for_queue
    @lock_resource
    def public_to_private(cls, _id, user, check=True):
        """
        Change the resource lock mode from public to private, return false if fail

        :param _id: the _id of resource and it is a uuid
        :param user: resource locked by who
        :param check: True for check if the status of resource is abnormal after changing
        :return:
        """
        resource_model = cls.get_resource_model()
        data = resource_model.find_one(filter={"_id": _id})
        if user not in data["user"]:
            raise res_except.ResourceLockException(
                "The resource(%(_id)s) is not locked by %(user)s, but locked by "
                "%(users)s" % {"_id": _id, "user": user, "users": data["user"]})

        if len(data["user"]) > 1:
            raise res_except.ResourceLockException(
                "The resource(%(_id)s) is locked by multiple users: %(user)s" %
                {"_id": _id, "user": data["user"]})

        if data["mode"] == "private":
            LOG.warning("The resource(%(_id)s) is already privately locked by %(user)s" %
                        {"_id": _id, "user": user})

        update_data = {
            "status": "busy",
            "mode": "private",
            "mutex": {},
            "update_time": Utils.now()
        }
        id_filter = {"_id": _id}
        resource_model.update_one(filter=id_filter,
                                  update={"$set": update_data})
        if check:
            new_res_info = resource_model.find_one(filter=id_filter)
            cls._check_resource_status(new_res_info)

    @classmethod
    def _check_mutex_modifiable(cls, _id, user, data):
        if user not in data["user"]:
            raise res_except.ResourceLockException(
                "The resource(%(_id)s) is not locked by %(user)s, but locked by "
                "%(users)s" % {"_id": _id, "user": user, "users": data["user"]})
        if data["mode"] != "public":
            raise res_except.ResourceLockException(
                "The resource(%(_id)s) mode must be public, but now is %(mode)s" %
                {"_id": _id, "mode": data["mode"]})

    @classmethod
    def _check_mutex_modifiable(cls, _id, user, data):
        if user not in data["user"]:
            raise res_except.ResourceLockException(
                "The resource(%(_id)s) is not locked by %(user)s, but locked by "
                "%(users)s" % {"_id": _id, "user": user, "users": data["user"]})
        if data["mode"] != "public":
            raise res_except.ResourceLockException(
                "The resource(%(_id)s) mode must be public, but now is %(mode)s" %
                {"_id": _id, "mode": data["mode"]})

    @classmethod
    @wait_for_queue
    @lock_resource
    def add_mutex(cls, _id, user, mutex, check=True):
        """
        Add the shared resource 'mutex' value for a user

        :param _id: the _id of resource and it is a uuid
        :param user: resource locked by who
        :param mutex: str or list, additional 'mutex' value
        :param check: True for check if the status of resource is abnormal after changing
        :return:
        """
        resource_model = cls.get_resource_model()
        data = resource_model.find_one(filter={"_id": _id})
        cls._check_mutex_modifiable(_id, user, data)

        if not isinstance(mutex, list):
            mutex = [mutex]

        res_mutex = data["mutex"]
        exist_mutex = set(mutex) & set(sum(res_mutex.values(), []))

        if exist_mutex:
            if user in res_mutex.keys() and set(exist_mutex).issubset(set(res_mutex[user])):
                if set(mutex).issubset(set(res_mutex[user])):
                    LOG.warning("The resource(%(_id)s) already has %(user)s's mutex: %(mutex)s" %
                                {"_id": _id, "user": user, "mutex": mutex})
                    return
            else:
                raise res_except.ResourceLockException(
                    "Failed to add mutex(%(user)s: %(mutex)s), now is %(res_mutex)s" %
                    {"mutex": mutex, "user": user, "res_mutex": res_mutex})

        if user not in res_mutex:
            res_mutex[user] = []
        res_mutex[user] += mutex
        res_mutex[user] = list(set(res_mutex[user]))
        update_data = {
            "mutex": res_mutex
        }
        id_filter = {"_id": _id}
        resource_model.update_one(filter=id_filter,
                                  update={"$set": update_data})
        if check:
            new_res_info = resource_model.find_one(filter=id_filter)
            cls._check_resource_status(new_res_info)

    @classmethod
    @wait_for_queue
    @lock_resource
    def remove_mutex(cls, _id, user, mutex, check=True):
        """
        Remove the shared resource 'mutex' value for a user

        :param _id: the _id of resource and it is a uuid
        :param user: resource locked by who
        :param mutex: str or list, removed 'mutex' value
        :param check: True for check if the status of resource is abnormal after changing
        :return:
        """
        resource_model = cls.get_resource_model()
        data = resource_model.find_one(filter={"_id": _id})
        cls._check_mutex_modifiable(_id, user, data)

        if not isinstance(mutex, list):
            mutex = [mutex]

        res_mutex = data["mutex"]
        if not (user in res_mutex.keys() and set(mutex).issubset(set(res_mutex[user]))):
            raise res_except.ResourceLockException(
                "The resource(%(_id)s) dose not include the mutex(%(user)s: %(mutex)s), "
                "now is: %(res_mutex)s" %
                {"_id": _id, "user": user, "mutex": mutex, "res_mutex": res_mutex})

        res_mutex[user] = list(set(res_mutex[user]) - set(mutex))
        if not res_mutex[user]:
            res_mutex.pop(user)
        update_data = {
            "mutex": res_mutex
        }
        id_filter = {"_id": _id}
        resource_model.update_one(filter=id_filter,
                                  update={"$set": update_data})
        if check:
            new_res_info = resource_model.find_one(filter=id_filter)
            cls._check_resource_status(new_res_info)

    @classmethod
    @lock_resource
    def clean_user(cls, _id, check=True):
        """
        Clean all users of the resource so it can be idle

        :param _id: the _id of resource and it is a uuid
        :param check: True for check if the status of resource is abnormal after cleaning
        """
        resource_model = cls.get_resource_model()
        update_data = {
            "user": [],
            "status": "idle",
            "mode": None,
            "mutex": {},
            "update_time": Utils.now()
        }
        id_filter = {"_id": _id}
        resource_model.update_one(filter=id_filter,
                                  update={"$set": update_data})
        if check:
            new_res_info = resource_model.find_one(filter=id_filter)
            cls._check_resource_status(new_res_info)

    @classmethod
    def _check_resource_status(cls, resource_info):
        """
        Check if status of resource is abnormal

        :param resource_info: full info of resource
        """
        ex = ValueError("Abnormal status of resource: %s" % resource_info)
        if len(resource_info["user"]) != len(set(resource_info["user"])):
            raise ex

        mode = resource_info["mode"]
        if mode == "private":
            if len(resource_info["user"]) != 1:
                raise ex
            if resource_info["status"] != "busy":
                raise ex
            if resource_info["mutex"] != {}:
                raise ex
        elif mode == "public":
            if len(resource_info["user"]) == 0:
                raise ex
            if resource_info["user_limit"] == 1:
                raise ex
            elif resource_info["user_limit"] == 0:
                if resource_info["status"] != "using":
                    raise ex
            else:
                if len(resource_info["user"]) > resource_info["user_limit"]:
                    raise ex
                elif len(resource_info["user"]) == resource_info["user_limit"]:
                    if resource_info["status"] != "busy":
                        raise ex
                elif resource_info["status"] != "using":
                    raise ex
        elif mode is None:
            if len(resource_info["user"]) != 0:
                raise ex
            if resource_info["status"] not in ["idle", "disabled"]:
                raise ex
            if resource_info["mutex"] != {}:
                raise ex
        else:
            raise ex

        if not set(resource_info["mutex"].keys()).issubset(set(resource_info["user"])):
            raise ex

        res_mutex = sum(resource_info["mutex"].values(), [])
        if len(res_mutex) > len(set(res_mutex)):
            raise ex

    @classmethod
    def get_resource_registered(cls, category=None, descriptor=None):
        resource_model = cls.get_resource_model()
        resources = resource_model.find(
            {"category": category, "descriptor": descriptor})
        return resources

    @classmethod
    def get_resource_schema_registered(cls, _id=None):
        resource_model = cls.get_resource_schema_model()
        resources = resource_model.find({"_id": _id})
        return resources

    @classmethod
    @lock_resource
    def register(cls, category, descriptor, status='idle', mode=None,
                 user=None, user_limit=0, _id=None, for_ci=False, check=True):
        """
        Register the resource for test automation

        :param category: the category of the resource
        :param descriptor: the descriptor of the resource
        :param status: the status of the resource. valid values are: idle | using | disabled
        :param mode: the mode of the resource. valid values are: public | private | None
        :param user: the list of users who haven locked
        :param user_limit: the max limit of the count of users using the resource at the same time. 0 means infinite
        :param _id: the _id of resource and it is a uuid
        :param for_ci: True means the resource is standard and only for automated test
        :param check: True for check if the status of resource is abnormal before register
        :return: the resource info of registered, the demo are as follows
        {
            u'_id': u'jmexfqk2310ys9ga7u40thz5'
            u'category': u'RESOURCE_CONTROLLER_TEST',
            u'mode': None,
            u'status': u'idle',
            u'user': [],
            u'user_limit': 0,
            u'for_ci': True,
            u'mutex': {},
            u'descriptor': {
                u'net_interface': u'eth3',
                u'host': u'node-240',
                u'ENV': u'env_abcdefghijklmnopqrstuvwxyz'
            },
            u'create_time': 1481027122.197588,
            u'update_time': 1481027122.197588,
        }
        """
        resource_model = cls.get_resource_model()
        if user is None:
            user = []
        if _id is None:
            _id = str(uuid.uuid4())

        t = Utils.now()
        data = {
            "category": category,
            "descriptor": descriptor,
            "mode": mode,
            "status": status,
            "user": user,
            "user_limit": user_limit,
            "create_time": t,
            "update_time": t,
            "for_ci": for_ci,
            "mutex": {},
            "_id": _id
        }
        common_schema = cls.get_resource_schema()
        resource_schema = cls.get_resource_schema(category)
        jsonschema.validate(data, common_schema)
        jsonschema.validate(data["descriptor"], resource_schema)

        already_data = cls.get_resource_registered(category=category,
                                                   descriptor=descriptor)
        already_data_count = already_data.count()
        if already_data_count == 1:
            LOG.warning("There are already %(count)s resource registered, "
                        "so it will not be registered again." % {
                            "count": already_data_count})
            resource = already_data.next()
            return resource
        if already_data_count > 1:
            LOG.error("There are %(count)s resources registered." % {
                "count": already_data_count})
            resource = already_data.next()
            return resource

        if check:
            cls._check_resource_status(data)
        insert_ret = resource_model.insert_one(data)
        resources = resource_model.find({"_id": insert_ret.inserted_id})
        resources_count = resources.count()
        if resources_count > 1:
            raise res_except.ResourceDuplicatedError(
                "There are %(count)s resources registered after register: "
                "%(ret)s" % {"count": resources_count, "ret": list(resources)})

        if resources_count == 0:
            raise ValueError(
                "The resource is still not registered after register, "
                "maybe error occured. Data to register: %s" % data)

        resource = resources.next()
        return resource

    @classmethod
    @lock_resource
    def unregister(cls, _id):
        """
        Unregister the resource, which will be deleted permanently

        :param _id: the _id of resource and it is a uuid
        """
        resource_model = cls.get_resource_model()
        data = resource_model.find_one(filter={"_id": _id})
        if (data["status"] in ["using", "busy"]) or \
                (data["mode"] is not None) or (len(data["user"]) != 0):
            raise res_except.ModifyResourceInUseError(data)
        ret = resource_model.delete_many(filter={"_id": _id})
        if not ret.raw_result.get("ok", None) == 1:
            raise res_except.ResourceLockException("some error occured, the result is %(result)s" %
                                                   {"result": ret.raw_result})

    @classmethod
    def get_resource_schema(cls, category='ALL_RESOURCE'):
        registered_data = cls.get_resource_schema_registered(_id=category)
        if registered_data.count() == 0:
            raise res_except.ResourceSchemaNotFoundError(category)
        else:
            return registered_data.next()['schema']

    @classmethod
    @lock_resource
    def update(cls, _id, update, force=False):
        """
        Update the existing resource

        :param _id: the _id of resource and it is a uuid
        :param update: the modifications to apply
        :param force: True for force update,  no matter what the status of resource is
        """
        resource_model = cls.get_resource_model()
        data = resource_model.find_one(filter={"_id": _id})
        if not force:
            if (data["status"] in ["using", "busy"]) or \
                    (data["mode"] is not None) or (len(data["user"]) != 0):
                raise res_except.ModifyResourceInUseError(data)

            for invalid_key in ["status", "mode", "user"]:
                if invalid_key in update:
                    raise ValueError("The \"%s\" value is not allowed to modify, "
                                     "please use lock() or unlock()" % invalid_key)

        cls._update_nested_dict(data, update)
        common_schema = cls.get_resource_schema()
        resource_schema = cls.get_resource_schema(data["category"])
        jsonschema.validate(data, common_schema)
        jsonschema.validate(data["descriptor"], resource_schema)

        ret = resource_model.update_one(filter={"_id": _id},
                                        update={"$set": data})
        if not ret.raw_result.get("ok", None) == 1:
            raise res_except.ResourceLockException("some error occured, the result is %(result)s" %
                                                   {"result": ret.raw_result})

        data = resource_model.find_one(filter={"_id": _id})
        return data

    @classmethod
    def _update_nested_dict(cls, data, update):
        for k, v in update.iteritems():
            if isinstance(v, collections.Mapping):
                data[k] = cls._update_nested_dict(data.get(k, {}), v)
            else:
                data[k] = v
        return data

    @classmethod
    def _update_nested_dict(cls, data, update):
        for k, v in update.iteritems():
            if isinstance(v, collections.Mapping):
                data[k] = cls._update_nested_dict(data.get(k, {}), v)
            else:
                data[k] = v
        return data

    @classmethod
    def initialize(cls, schema_list, schema_for_schema=None, clean=False):
        """
        Initialize or update the collection of resource schema in mongodb

        :param schema_list: list of resource schema to save in mongodb
        :param schema_for_schema: the schema for resource schema
        :param clean: clean all schemas at the beginning
        """
        resource_schema_model = cls.get_resource_schema_model()
        if clean:
            resource_schema_model.remove()
            LOG.info("Collection of resource_schema has been cleaned.")

        for resource_schema in schema_list:
            if schema_for_schema:
                jsonschema.validate(resource_schema, schema_for_schema)
            registered_data = cls.get_resource_schema_registered(
                _id=resource_schema["_id"])

            if registered_data.count() == 0:
                resource_schema_model.insert_one(resource_schema)
            elif registered_data.count() > 1:
                raise ValueError(
                    "Resource schema of %(category)s is multiple, which is "
                    "abnormal: %(schema)s" % {
                        "category": resource_schema["_id"],
                        "schema": resource_schema["schema"]})
            else:
                LOG.info(
                    "Resource schema of %(category)s is registered and will be "
                    "updated. Original schema: %(schema)s" % {
                        "category": resource_schema["_id"],
                        "schema": resource_schema["schema"]})
                resource_schema_model.update_one(
                    filter={"_id": resource_schema["_id"]},
                    update={"$set": resource_schema})

    @classmethod
    def filter(cls, resource_filter, user,
               user_own=False, except_resource=None,
               rule_map=None, quiet=True, ignore_status=False):
        """
        Search the resources that meet the filter rules

        :param resource_filter: a dict in "resource_name: filter_rules" format
        :param user: the busy resource owned by user is considered available
        :param user_own: True for only search resources that user has owned
        :param except_resource: a list of _id of resource to be remove from search scope
        :param rule_map: a dict in "abstract_rule: advanced_rule" format
        :param quiet: True for no log output
        :param ignore_status: return suitable resources even if they are busy or using
        :return: the dict of filtered in "resource_name: resource_info" format
        """
        # LOG.debug("All resources: %s" % list(cls.list()))
        resource_model = cls.get_resource_model()
        original_filter = resource_filter
        resource_filter = copy.deepcopy(original_filter)
        cls.resource_filter = original_filter
        cls.user = user

        if rule_map is None:
            rule_map = {}
        cls._transform_advanced_rule(resource_filter, rule_map)
        if not quiet:
            LOG.info("Transformed filter: %s" % resource_filter)
        relation_map = cls._get_relation_map(resource_filter)
        confirm_order = cls._get_confirm_order(resource_filter, relation_map)
        cls.confirm_order = confirm_order
        if not quiet:
            LOG.info("Confirm order: %s" % confirm_order)

        resource_confirmed = dict(zip(resource_filter.keys(),
                                      [None] * len(resource_filter)))
        if except_resource:
            if isinstance(except_resource, list):
                resource_used_list = except_resource
            else:
                raise TypeError("except_resource should be a list, "
                                "but it is a %(type)s: %(except)s"
                                % {"type": type(except_resource),
                                   "except": except_resource})
        else:
            resource_used_list = []

        cls.deepest_index = -1
        cls.resources_db = {}
        ret = cls._confirm_resource(user,
                                    user_own,
                                    resource_filter,
                                    relation_map,
                                    confirm_order,
                                    0,
                                    resource_confirmed,
                                    resource_used_list,
                                    resource_model,
                                    ignore_status)
        if not ret:
            raise res_except.ResourceNotFoundError(cls)

        return resource_confirmed

    @classmethod
    def _transform_advanced_rule(cls, resource_filter, rule_map):
        """
        Transform advanced_rule so that all rules can be understood and executed

        :param resource_filter: a dict in "resource_name: filter_rules" format
        :param rule_map: a dict in "abstract_rule: advanced_rule" format
        :return:
        """
        # Transform abstract rules into base and advanced rules
        for res, filter in resource_filter.items():
            if len(filter) == 0:
                raise ValueError("Filter of %(res)s is invalid: %(filter)s" %
                                 {"res": res, "filter": filter})

            if "advanced_rule" not in filter:
                continue
            new_rule_type = dict(zip(
                ["delete", "advanced_rule", "advanced_rule_for_list"],
                [[] for i in range(3)]))
            for rule in filter["advanced_rule"]:
                for regex, info in rule_map.items():
                    ret = re.match(regex, rule)
                    if not ret:
                        continue
                    new_rule_type["delete"].append(rule)
                    parameter = dict(zip(info["parameter"], ret.groups()))
                    parameter["res"] = res

                    new_rule = info["rule"].copy()
                    new_base_rule = new_rule
                    new_ad_rule = {}
                    for rule_type in ["advanced_rule",
                                      "advanced_rule_for_list"]:
                        if rule_type in new_base_rule:
                            new_ad_rule[rule_type] = new_base_rule.pop(
                                rule_type)
                        else:
                            new_ad_rule[rule_type] = None

                    for key, value in new_base_rule.items():
                        if isinstance(value, str):
                            value = value % parameter
                        if key in filter and filter[key] != value:
                            LOG.warning(
                                "Conflict value of %(key)s, old value is "
                                "%(n_v)s given by '%(rule)s', but new value "
                                "%(o_v)s will be used" % {"key": key, "n_v": value,
                                                          "rule": rule, "o_v": filter[key]})
                        else:
                            filter[key] = value

                    for rule_type in ["advanced_rule",
                                      "advanced_rule_for_list"]:
                        if new_ad_rule[rule_type] is None:
                            continue
                        for i, ad_rule in enumerate(new_ad_rule[rule_type]):
                            ad_rule = ad_rule % parameter
                            new_rule_type[rule_type].append(ad_rule)

            for rule in new_rule_type["delete"]:
                filter["advanced_rule"].remove(rule)
            for rule_type in ["advanced_rule", "advanced_rule_for_list"]:
                if rule_type in filter:
                    filter[rule_type] += new_rule_type[rule_type]
                else:
                    filter[rule_type] = new_rule_type[rule_type]
            # Transform variables in advanced rule into value in resource_confirmed
        for filter in resource_filter.values():
            for rule_type in ["advanced_rule", "advanced_rule_for_list"]:
                advanced_rule = filter.get(rule_type)
                if advanced_rule is None:
                    continue
                transformed_rules = []
                for rule in advanced_rule:
                    rule = rule.replace("'", '"')
                    new_rule = re.sub(r"(\b(?!\b_id\b)_\w+\b)",
                                      r'resource_confirmed["\g<1>"]', rule)
                    if new_rule in transformed_rules:
                        LOG.warning("Duplicate rules detected: %s" % new_rule)
                    else:
                        transformed_rules.append(new_rule)
                filter[rule_type] = transformed_rules

    @classmethod
    def _get_confirm_order(cls, resource_filter, relation_map):
        """
        Get all the resources related to the resource

        :param resource_filter: a dict in "resource_name: filter_rules" format
        :param relation_map: a dict in "resource_name: relative_resource" format
        :return: the list of resources with the order to confirm
        """
        resource_count = {}
        for res, filter in resource_filter.items():
            if "count" in filter:
                resource_count[res] = filter["count"]
            else:
                resource_count[res] = 0

        # sort resources in order of dependency
        all_resource = resource_filter.keys()
        confirm_order = []
        while True:
            new_order = []
            for res in all_resource:
                related_res = relation_map[res]
                depend_res_list = [rs for rs in related_res if
                                   rs not in confirm_order]
                if len(depend_res_list) == 0:
                    new_order.append(res)
            if len(new_order) == 0:
                break
            else:
                for res in new_order:
                    all_resource.remove(res)
                new_order.sort(key=lambda x: resource_count[x])
                confirm_order += new_order

        # there may be left resources which depend on each other
        if len(all_resource) != 0:
            raise ValueError("There are resources that use nonexistent key in "
                             "filter in advanced rule or depend on each other: "
                             "%(res)s, resource filter: %(filter)s"
                             % {"res": all_resource, "filter": resource_filter})

        # if the resource is totally independent, put it to last
        resource_dependent_map = dict(zip(
            relation_map.keys(), [[] for i in range(len(relation_map))]))
        for res, depend in relation_map.items():
            for dp in depend:
                resource_dependent_map[dp].append(res)
        independent_res = []
        for res in confirm_order:
            if len(relation_map[res]) == 0 and (
                    len(resource_dependent_map[res]) == 0):
                independent_res.append(res)
        for res in independent_res:
            confirm_order.remove(res)
        confirm_order += independent_res

        # sample of ordered_related_resource: [res_1, res2, [res3, res4]]
        # as above, res3 and res4 depend on each other
        return confirm_order

    @classmethod
    def _get_relation_map(cls, resource_filter):
        """
        Get the relation map of all resources, and transform variables in advanced_rule

        :param resource_filter: a dict in "resource_name: filter_rules" format
        :return: the dict of relation map, such as: {res1: [res2], res2: [res1, res5] ...}
        """
        relation_map = {}
        for res, filter in resource_filter.items():
            relative_res = []
            for rule_type in ["advanced_rule", "advanced_rule_for_list"]:
                advanced_rule = filter.get(rule_type)
                if advanced_rule:
                    for rule in advanced_rule:
                        depend_res_list = re.findall(
                            r'\bresource_confirmed\["((?!\b_id\b)_\w+)"\]',
                            rule)
                        new_depend_res = [rs for rs in depend_res_list if
                                          rs not in relative_res and rs != res]
                        relative_res += new_depend_res

            relation_map[res] = relative_res

        return relation_map

    @classmethod
    def _confirm_resource(cls, user, user_own, resource_filter, relation_map,
                          confirm_order, index, resource_confirmed,
                          resource_used_list, resource_model, ignore_status):
        """
        Confirm resources in the list of confirm_order

        :param user: the busy resource owned by user is considered available
        :param user_own: True for only search resources that user has owned
        :param resource_filter: a dict in "resource_name: filter_rules" format
        :param relation_map: a dict in "resource_name: relative_resource" format
        :param confirm_order: the list of resource to confirm in order
        :param index: the index of confirm_order, to mark the resource to confirm currently
        :param resource_confirmed: the dict of already confirmed resource in "name: info" format
        :param resource_used_list: the list of _id of already confirmed resource
        :param resource_model: the mongodb object of resource collection
        :param ignore_status: return suitable resources even if they are busy or using
        :return: True for confirm success, False for fail
        """
        if index >= len(confirm_order):
            return True

        if index > cls.deepest_index:
            cls.deepest_index = index

        resource_name = confirm_order[index]
        filter = resource_filter[resource_name]
        base_rule = filter.copy()
        count = base_rule.get("count")
        advanced_rule = base_rule.get("advanced_rule")
        if "advanced_rule" in base_rule:
            base_rule.pop("advanced_rule")
        advanced_rule_for_list = base_rule.get("advanced_rule_for_list")
        if "advanced_rule_for_list" in base_rule:
            base_rule.pop("advanced_rule_for_list")

        resource_available = cls._get_resource_available(
            user, user_own, resource_name, relation_map,
            base_rule, advanced_rule, resource_confirmed, resource_used_list,
            resource_model, ignore_status=ignore_status)

        for resource in resource_available:
            if isinstance(resource, tuple):
                resource = list(resource)

            resource_confirmed[resource_name] = resource
            if count is None:
                if not cls._check_advanced_rule(resource_name,
                                                advanced_rule,
                                                resource_confirmed):
                    continue
            if advanced_rule_for_list:
                if not cls._execute_advanced_rule(advanced_rule_for_list,
                                                  resource_confirmed):
                    continue

            if isinstance(resource, dict):
                resource_used_list.append(resource['_id'])
            elif isinstance(resource, list):
                for res in resource:
                    resource_used_list.append(res['_id'])
            else:
                raise res_except.ResourceLockException("Fail to get resource from "
                                                       "resource_available %s" % resource_available)

            if cls._confirm_resource(user,
                                     user_own,
                                     resource_filter,
                                     relation_map,
                                     confirm_order,
                                     index + 1,
                                     resource_confirmed,
                                     resource_used_list,
                                     resource_model,
                                     ignore_status):
                return True
            else:
                if isinstance(resource, dict):
                    resource_used_list.remove(resource['_id'])
                elif isinstance(resource, list):
                    for res in resource:
                        resource_used_list.remove(res['_id'])

        resource_confirmed[resource_name] = None
        return False

    @classmethod
    def _get_resource_available(
            cls, user, user_own, resource_name, relation_map,
            base_rule, advanced_rule, resource_confirmed, resource_used_list,
            resource_model, ignore_status):
        """
        Get resources of single type that meet the base filter rules

        :param user: the busy resource owned by user is considered available
        :param user_own: True for only search resources that user has owned
        :param resource_name: the name of resource
        :param relation_map: a dict in "resource_name: relative_resource" format
        :param base_rule: filter without advanced rules
        :param advanced_rule: the list of advanced rules
        :param resource_confirmed: the dict of already confirmed resource in "name: info" format,
        used in eval or exec
        :param resource_used_list: the list of _id of already confirmed resource
        :param resource_model: the mongodb object of resource collection
        :param ignore_status: return suitable resources even if they are busy or using
        :return: a list of all available resources of single type
        """
        for key in ["count", "public", "mutex"]:
            if base_rule.get(key) is None:
                base_rule[key] = None
        public = base_rule.pop("public")
        mutex = base_rule.pop("mutex")
        if not public and mutex:
            raise ValueError("If resource is to lock privately, the 'mutex' shouldn't be given")

            if ignore_status:
                available_status = ["idle", "using", "busy"]
        else:
            available_status = ["idle", "using"] if public else ["idle"]

        count = base_rule.pop("count")
        if user_own and count not in [-1, 0]:
            base_rule["user"] = user
        base_rule["status"] = {"$ne": "disabled"}
        if resource_name not in cls.resources_db:
            cls.resources_db[resource_name] = list(
                resource_model.find(base_rule).sort("create_time", pymongo.ASCENDING))
            if ignore_status:
                for res in cls.resources_db[resource_name]:
                    res["user"] = []
            elif mutex:  # public must be true
                for res in cls.resources_db[resource_name]:
                    res_mutex = res["mutex"]
                    exist_mutex = set(mutex) & set(sum(res_mutex.values(), []))
                    if not exist_mutex:
                        continue
                    if user in res_mutex.keys() and set(exist_mutex).issubset(set(res_mutex[user])):
                        continue

                    if user in res["user"]:
                        res["user"].remove(user)
                    res["status"] = "busy"

        resources_db = cls.resources_db[resource_name]
        resources = []
        for res in resources_db:
            if res['_id'] not in resource_used_list:
                resources.append(res)

        # Use two list in order to put resources owned in front
        resources_checked = []  # resources checked and not owned by user
        resources_owned = []  # resources checked and owned by user
        if count is None:
            for resource in resources:
                if user in resource["user"]:
                    if public or resource['mode'] == 'private' or resource["user"] == [user] \
                            or ignore_status:
                        resources_owned.append(resource)
                else:
                    if resource["status"] not in available_status:
                        continue
                    resources_checked.append(resource)
            return resources_owned + resources_checked  # [res_1, res_2, ...]

        else:
            for resource in resources:
                resource_confirmed[resource_name] = resource
                if cls._check_advanced_rule(resource_name, advanced_rule,
                                            resource_confirmed):
                    if user in resource["user"]:
                        if public or resource['mode'] == 'private' or resource["user"] == [user] \
                                or ignore_status:
                            resources_owned.append(resource)
                    else:
                        if resource["status"] not in available_status:
                            if count == -1:
                                return []
                            continue
                        resources_checked.append(resource)

            resources_checked = resources_owned + resources_checked
            # 0 means at least one res, -1 means all res
            if count <= 0:
                if len(resources_checked) != 0:
                    return [resources_checked]  # [[res_1, res_2, ...]]
                elif len(relation_map[resource_name]) == 0:
                    raise res_except.ResourceNotFoundError(cls)
                else:
                    return []

            # 1, 2, 3 ... means the exact count of res
            if count > len(resources_checked) and \
                    (len(relation_map[resource_name]) == 0):
                raise res_except.ResourceNotFoundError(cls)
            return itertools.permutations(resources_checked, count)

    classmethod
    def _check_advanced_rule(cls, resource_name, advanced_rule,
                             resource_confirmed):
        """
        Check if all advanced rules pass

        :param resource_name: the name of resource
        :param advanced_rule: the list of advanced rules
        :param resource_confirmed: the dict of already confirmed resource in "name: info" format,
        used in eval or exec
        :return: True for check success, False for fail
        """
        if advanced_rule is None:
            return True

        resource = resource_confirmed[resource_name]
        if isinstance(resource, list):
            old_resource = list(resource)
            for res in resource:
                resource_confirmed[resource_name] = res
                if not cls._execute_advanced_rule(advanced_rule,
                                                  resource_confirmed):
                    return False

            resource_confirmed[resource_name] = old_resource
            return True
        else:
            return cls._execute_advanced_rule(advanced_rule, resource_confirmed)

    @classmethod
    def _execute_advanced_rule(cls, advanced_rule, resource_confirmed):
        """
        Execute all advanced rules and return result

        :param advanced_rule: the list of advanced rules
        :param resource_confirmed: the dict of already confirmed resource in "name: info" format,
        used in eval or exec
        :return: True for execute success, False for fail
        """
        global_var = {
            "resource_confirmed": resource_confirmed
        }
        for index, rule in enumerate(advanced_rule):
            try:
                if not eval(rule, global_var):
                    return False
            except SyntaxError as e:
                if 'invalid syntax' in str(e):
                    exec
                    rule
                    # Get new variable like "ids = res["ids"]" in rule
                    temp_var_list = re.findall(r"\b(\w+)\b(?:\s+)?=(?:\s+)?\w+",
                                               rule)
                    for temp_var in temp_var_list:
                        global_var.update({temp_var: locals()[temp_var]})
                else:
                    LOG.error("advanced rule: %s" % rule)
                    raise
            except:
                LOG.error("advanced rule: %s" % rule)
                raise
        return True

    @classmethod
    def _merge_separated_resource(cls, resource_confirmed):
        """
        Merge the resources whose names are res_1, res_2, ...

        :param resource_confirmed: the dict of already confirmed resource in "name: info" format
        :return:
        """
        to_merge_count = {}
        for name in resource_confirmed:
            ret = re.match(r"^(\w+)_\d+$", name)
            if ret:
                base_name = ret.group(1)
                if base_name in to_merge_count:
                    to_merge_count[base_name] += 1
                else:
                    to_merge_count[base_name] = 1

        orginal_name_list = resource_confirmed.keys()
        for base_name in to_merge_count:
            index = 1
            resource_confirmed[base_name] = []
            while True:
                name = base_name + "_%d" % index
                if name in resource_confirmed:
                    resource_confirmed[base_name].append(
                        resource_confirmed.pop(name))
                    index += 1
                else:
                    break
            if (index - 1) != to_merge_count[base_name]:
                raise ValueError("Merger not completed because names like "
                                 "\"%(name)s_*\" are incorrect in %(list)s" %
                                 {"name": base_name, "list": orginal_name_list})


class Queue(object):

    def __init__(self):
        pass

    @classmethod
    def get_queue_model(cls):
        queue_model = models.Queue()
        return queue_model.collection

    @classmethod
    # @lock_queue
    def push(cls, user=None):
        queue_model = cls.get_queue_model()
        data = {"_id": user}
        count = queue_model.find(data).count()
        if count > 0:
            raise res_except.ResourceLockException("The user(%(user)s) is already in the queue" %
                                                   {"user": user})

        data["create_time"] = Utils.now()
        queue_model.insert_one(data)
        return data

    @classmethod
    # @lock_queue
    def pop(cls, user=None):
        queue_model = cls.get_queue_model()
        data = {"_id": user}
        ret = queue_model.delete_many(data)
        if ret.deleted_count == 0:
            LOG.warning("The user(%(user)s) do not in the queue" % {"user": user})
        return ret

    @classmethod
    def list(cls, user=None, sort=pymongo.ASCENDING):
        queue_model = cls.get_queue_model()
        query_filter = {}
        if user is not None:
            query_filter["_id"] = user
        queues = queue_model.find(query_filter).sort("create_time", sort)
        return queues

    @classmethod
    def index_of(cls, user):
        full_queue = list(cls.list())
        for index, element in enumerate(full_queue):
            if element["_id"] == user:
                return index, full_queue[0]

        return None, None
