class ResourceLockException(Exception):
    pass


class UserNotInUsersWhenUnlock(ResourceLockException):
    pass


class ResourceDuplicatedError(ResourceLockException):
    pass


class UserQueueError(ResourceLockException):
    pass


class ModifyResourceInUseError(ResourceLockException):
    def __init__(self, resource):
        self.resource = resource
        self.message = self.__str__()

    def __str__(self):
        return "Resource in use is not allowed to modify: %s" % self.resource


class ResourceSchemaNotFoundError(ResourceLockException):
    def __init__(self, category):
        self.category = category
        self.message = self.__str__()

    def __str__(self):
        return "Resource schema of %s is not found" % self.category


class ResourceNotFoundError(ResourceLockException):
    def __init__(self, resource_class):
        self.type = resource_class.confirm_order[resource_class.deepest_index]
        self.order = resource_class.confirm_order
        self.user = resource_class.user
        self.filter = resource_class.resource_filter
        self.message = self.__str__()

    def __str__(self):
        return ("Failed to find %(type)s, order: %(order)s, user: %(user)s, "
                "filter: %(filter)s" % {"type": self.type,
                                        "order": self.order,
                                        "user": self.user,
                                        "filter": self.filter})


class ResouceNotExistError(ResourceLockException):
    def __init__(self, resource_not_found_error):
        self.type = resource_not_found_error.type
        self.order = resource_not_found_error.order
        self.user = resource_not_found_error.user
        self.filter = resource_not_found_error.filter
        self.message = resource_not_found_error.message

    def __str__(self):
        return ("There is no suitable %(type)s, order: %(order)s, user: %(user)s, "
                "filter: %(filter)s" % {"type": self.type,
                                        "order": self.order,
                                        "user": self.user,
                                        "filter": self.filter})