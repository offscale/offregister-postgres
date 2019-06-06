from collections import namedtuple, Iterable
from functools import partial
from urlparse import urlparse, ParseResult

from fabric.context_managers import settings
from fabric.operations import sudo
from offutils import ensure_quoted


def get_postgres_params(parsed_connection_str):  # type: (str or ParseResult) -> str
    if not isinstance(parsed_connection_str, ParseResult):
        parsed_connection_str = urlparse(parsed_connection_str)

    return ' '.join(('--host={}'.format(parsed_connection_str.hostname or 'localhost'),
                     '--port={}'.format(parsed_connection_str.port or 5432),
                     '--username={}'.format(parsed_connection_str.username or 'postgres')))


def setup_users(username='postgres', dbs=None, users=None, cluster=False, cluster_conf=None,
                superuser=False, connection_str='', **kwargs):
    postgres = partial(sudo, user=username, shell_escape=True)

    Create = namedtuple('Create', ('user', 'password', 'dbname'))

    # parsed_conn_str = urlparse(connection_str[1:-1])

    def create(user):
        make = Create(**user)
        if not make.user or not make.password or not make.dbname:
            raise AttributeError('To create a user, you must include user, password and dbname')

        fmt = {}

        database_uri = ensure_quoted(connection_str)

        if postgres(
            "psql -t -c '\\du' {database_uri} | grep -Fq {user}".format(
                database_uri=database_uri, user=make.user), warn_only=True).failed:
            fmt['user'] = make.user
            if make.password:
                postgres("""psql {database_uri} -c "CREATE USER {user} WITH PASSWORD {password}"; """.format(
                    database_uri=database_uri, user=make.user, password=ensure_quoted(make.password)
                ))
            else:
                postgres('createuser {user}'.format(user=make.user))
        else:
            fmt['user'] = None

        if superuser:
            postgres(
                "psql {database_uri} -c 'ALTER USER {user} WITH SUPERUSER;'".format(database_uri=database_uri,
                                                                                    user=make.user))

        if len(
            postgres("""psql {database_uri} -tAc "SELECT 1 FROM pg_database WHERE datname={db}";""".format(
                database_uri=database_uri, db=ensure_quoted(make.dbname)))) == 0:
            postgres('createdb {db}'.format(db=make.dbname))
            fmt['db'] = make.dbname
        else:
            fmt['db'] = None

        postgres("""psql {database_uri} -c "GRANT ALL PRIVILEGES ON DATABASE {db} TO {user};" """.format(
            database_uri=database_uri, db=make.dbname, user=make.user
        ))

        return 'User: {user}; DB: {db}; granted'.format(**fmt)

    if 'kwargs' in kwargs:  # Weird edge case here, ** should unroll the dictionary?
        kwargs = kwargs['kwargs']
    assert kwargs.get('create') is not None and len(kwargs['create']), 'create not found in {}'.format(kwargs)
    assert isinstance(kwargs['create'], Iterable) and not isinstance(kwargs['create'], basestring)
    if 'create' in kwargs:
        return map(create, kwargs['create'])

    # TODO: Remove all below
    # if users is not None or dbs is not None:
    #     from offregister_postgres.ubuntu import _cluster_with_pgpool
    #
    #     def require_db(db):
    #         if len(
    #             postgres("""psql {database_uri} -tAc "SELECT 1 FROM pg_database WHERE datname='{db}'";""".format(
    #                 connection_str=connection_str, db=db))) == 0:
    #             postgres('createdb {db}'.format(db=db))
    #
    #     def require_user(user):
    #         if postgres('''psql -t -c '\du' "{database_uri}" | grep -Fq {user}'''.format(
    #             connection_str=connection_str, user=user['name']
    #         ), warn_only=True).failed:
    #             params = get_postgres_params(parsed_conn_str)
    #             with settings(prompts={'Password: ': parsed_conn_str.password}):
    #                 postgres('createuser {params} --superuser {user}'.format(params=params, user=user['name']))
    #
    #     if cluster:
    #         if not cluster_conf:
    #             raise ValueError('Cannot cluster without custom conf')
    #         _cluster_with_pgpool(cluster_conf)
    #
    #     return {'dbs': map(require_db, dbs), 'users': map(require_user, users)}
