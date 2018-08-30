from collections import namedtuple
from functools import partial

from fabric.operations import sudo


def setup_users(username='postgres', dbs=None, users=None, cluster=False, cluster_conf=None,
                superuser=False, **kwargs):
    postgres = partial(sudo, user=username)

    Create = namedtuple('Create', ('user', 'password', 'dbname'))

    def create(user):
        make = Create(**user)
        fmt = {}

        if len(postgres("psql -tAc '\du {user}'".format(user=make.user))) == 0:
            fmt['user'] = make.user
            if make.password:
                postgres('''psql -c "CREATE USER {user} WITH PASSWORD '{password}'";'''.format(
                    user=make.user, password=make.password
                ))
            else:
                postgres('createuser {user}'.format(user=make.user))

        else:
            fmt['user'] = None

        if superuser:
            postgres('psql -c "ALTER USER {user} WITH SUPERUSER;"'.format(user=make.user))

        if len(postgres("psql -tAc '\l {db}'".format(db=make.dbname))) == 0:
            postgres('createdb {db}'.format(db=make.dbname))
            fmt['db'] = make.dbname
        else:
            fmt['db'] = None

        postgres('psql -c "GRANT ALL PRIVILEGES ON DATABASE {db} TO {user};"'.format(
            db=make.dbname, user=make.user
        ))

        return 'User: {user}; DB: {db}; granted'.format(**fmt)

    if 'create' in kwargs:
        return map(create, kwargs['create'])

    # TODO: Remove all below
    if users is not None or dbs is not None:
        from offregister_postgres.ubuntu import _cluster_with_pgpool

        def require_db(db):
            if len(postgres("psql -tAc '\l {db}'".format(db=db))) == 0:
                postgres('createdb {db}'.format(db=db))

        def require_user(user):
            if len(postgres("psql -tAc '\du {user}'".format(user=user))) == 0:
                postgres('createuser --superuser {user}'.format(user=user))

        if cluster:
            if not cluster_conf:
                raise ValueError('Cannot cluster without custom conf')
            _cluster_with_pgpool(cluster_conf)

        return {'dbs': map(require_db, dbs), 'users': map(require_user, users)}
