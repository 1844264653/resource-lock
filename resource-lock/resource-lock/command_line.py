import click

from resource_lock import Resources


@click.group()
def res():
    pass


@res.command()
@click.option("--schema_list", required=True,
              help="list of resource schema to save in mongodb")
@click.option("--schema_for_schema", default=None,
              help="the schema for resource schema")
@click.option("--clean", default=False, type=bool,
              help="clean all schemas at the beginning")
def initialize(schema_list, schema_for_schema=None, clean=False):
    """
    Initialize or update the collection of resource schema in mongodb
    """
    schema_list = eval(schema_list)
    local = locals()
    converted = {}
    for param in ["schema_for_schema"]:
        if local[param]:
            converted[param] = eval(local[param])
    ret = Resources.initialize(schema_list=schema_list, clean=clean, **converted)
    if ret is not None:
        print ret



@res.command()
@click.option("--category", required=True,
              help="the category of the resource")
@click.option("--descriptor", required=True,
              help="the descriptor of the resource")
@click.option("--status", default='idle',
              help="the status of the resource. valid values are: idle | using | disabled")
@click.option("--mode", default=None,
              help="the mode of the resource. valid values are: public | private | None")
@click.option("--user", default=None,
              help="the list of users who haven locked")
@click.option("--user_limit", default=0, type=int,
              help="the max limit of the count of users using the resource at the same time. "
                   "0 means infinite")
@click.option("--id", default=None,
              help="the _id of resource and it is a uuid")
@click.option("--check", default=True,
              help="True for check if the status of resource is abnormal before register")
def register(category, descriptor, status='idle', mode=None,
             user=None, user_limit=0, id=None, check=True):
    """
    Register the resource for test automation
    """
    category = eval(category)
    descriptor = eval(descriptor)
    local = locals()
    converted = {}
    for param in ["user"]:
        if local[param]:
            converted[param] = eval(local[param])
    ret = Resources.register(category=category, descriptor=descriptor, status=status,
                             mode=mode, user_limit=user_limit, _id=id, check=check, **converted)
    if ret is not None:
        print ret


@res.command()
@click.option("--id", required=True,
              help="the _id of resource and it is a uuid")
def unregister(id):
    """
    Unregister the resource, which will be deleted permanently
    """
    ret = Resources.unregister(_id=id)
    if ret is not None:
        print ret


@res.command()
@click.option("--id", required=True,
              help="the _id of resource and it is a uuid")
@click.option("--update", required=True,
              help="the modifications to apply")
def update(id, update):
    """
    Update the existing resource
    """
    update = eval(update)
    ret = Resources.update(_id=id, update=update)
    if ret is not None:
        print ret


@res.command()
@click.option("--query_filter", default=None,
              help="a dict to describe query filter rules(mongodb syntax)")
def list(query_filter=None):
    """
    List the resources that meet the query filter
    """
    local = locals()
    converted = {}
    for param in ["query_filter"]:
        if local[param]:
            converted[param] = eval(local[param])

    ret = Resources.list(**converted)
    if ret is not None:
        print ret


@res.command()
@click.option("--resource_filter", required=True,
              help="a dict in \"resource_name: filter_rules\" format")
@click.option("--user", required=True,
              help="the busy resource owned by user is considered available")
@click.option("--user_own", default=False,
              help="True for only search resources that user has owned")
@click.option("--except_resource", default=None,
              help="a list of _id of resource to be remove from search scope")
@click.option("--rule_map", default=None,
              help="a dict of abstract_rule:advanced_rule")
def filter(resource_filter, user, user_own=False, except_resource=None, rule_map=None):
    """
    Search the resources that meet the filter rules
    """
    local = locals()
    converted = {}
    for param in ["except_resource", "rule_map"]:
        if local[param]:
            converted[param] = eval(local[param])
    ret = Resources.filter(resource_filter=resource_filter,
                           user=user, user_own=user_own, **converted)
    if ret is not None:
        print ret

@res.command()
@click.option("--resource_filter", required=True,
              help="a dict in \"resource_name: filter_rules\" format")
@click.option("--user", required=True,
              help="resources locked by who")
@click.option("--merge", default=True, type=bool,
              help="True for merge res_1 and res_2 into res when return")
@click.option("--except_resource", default=None,
              help="a list of _id of resource to be remove from search scope")
@click.option("--rule_map", default=None,
              help="a dict of abstract_rule:advanced_rule")
def lock(resource_filter, user, merge=True, except_resource=None, rule_map=None):
    """
    Lock resources in mongodb according to resource_filter
    """
    resource_filter = eval(resource_filter)
    local = locals()
    converted = {}
    for param in ["except_resource", "rule_map"]:
        if local[param]:
            converted[param] = eval(local[param])
    ret = Resources.lock(resource_filter=resource_filter, user=user,
                         merge=merge, **converted)
    if ret is not None:
        print ret


@res.command()
@click.option("--user", required=True,
              help="resources locked by who")
@click.option("--filter", default=None,
              help="a dict to describe resources filter rules(mongodb syntax) for resource")
@click.option("--resource_list", default=None,
              help="a list of _id or full info of resources. "
                   "Such as [{res_1}, {res_2}, ...] or [res_1_id, ...]")
@click.option("--resource_dict", default=None,
              help=" a dict of resources returned by lock()"
                   "such as {{'res_1': {'_id': ...}, {'res_2': {...}}, ...}")
def unlock(user, filter=None, resource_list=None, resource_dict=None):
    """
    Unlock the resource locked by user
    """
    local = locals()
    converted = {}
    for param in ["filter", "resource_list", "resource_dict"]:
        if local[param]:
            converted[param] = eval(local[param])
    ret = Resources.unlock(user=user, **converted)
    if ret is not None:
        print ret


@res.command()
@click.option("--id", required=True,
              help="the _id of resource and it is a uuid")
@click.option("--user", required=True,
              help="resource locked by who")
def public_to_private(id, user, check=True):
    """
    Change the resource lock mode from public to private, return false if fail
    """
    ret = Resources.public_to_private(_id=id, user=user)
    if ret is not None:
        print ret


@res.command()
@click.option("--id", required=True,
              help="the _id of resource and it is a uuid")
@click.option("--user", required=True,
              help="resource locked by who")
@click.option("--mutex", required=True,
              help="str or list, additional 'mutex' value")
def add_mutex(id, user, mutex):
    """
    Add the shared resource 'mutex' value for a user
    """
    if '[' in mutex:
        mutex = eval(mutex)
    ret = Resources.add_mutex(_id=id, user=user, mutex=mutex)
    if ret is not None:
        print ret


@res.command()
@click.option("--id", required=True,
              help="the _id of resource and it is a uuid")
@click.option("--user", required=True,
              help="resource locked by who")
@click.option("--mutex", required=True,
              help="str or list, removed 'mutex' value")
def remove_mutex(id, user, mutex):
    """
    Remove the shared resource 'mutex' value for a user
    """
    if '[' in mutex:
        mutex = eval(mutex)
    ret = Resources.remove_mutex(_id=id, user=user, mutex=mutex)
    if ret is not None:
        print ret


@res.command()
@click.option("--id", required=True,
              help="the _id of resource and it is a uuid")
def clean_user(id, check=True):
    """
    Clean all users of the resource so it can be idle
    """
    ret = Resources.clean_user(_id=id)
    if ret is not None:
        print ret