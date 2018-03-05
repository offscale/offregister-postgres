from collections import namedtuple
from functools import partial

from fabric.api import sudo
from fabric.context_managers import settings
from fabric.contrib.files import upload_template, append

from offregister_fab_utils.apt import apt_depends
from offregister_fab_utils.fs import cmd_avail
from offregister_fab_utils.ubuntu.systemd import restart_systemd


def install0(version='9.6', username='postgres', dbs=None, users=None,
             extra_deps=tuple(), cluster=False, cluster_conf=None,
             superuser=False, **kwargs):
    ver = sudo("dpkg-query --showformat='${Version}' --show postgresql-9.6", warn_only=True)
    if ver.failed or not ver.startswith(version):
        append('/etc/apt/sources.list.d/pgdg.list', 'deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main',
               use_sudo=True)
        sudo('wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -')

        apt_depends('postgresql-{version}'.format(version=version),
                    'postgresql-contrib-{version}'.format(version=version),
                    'postgresql-server-dev-{version}'.format(version=version),
                    *extra_deps)
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


def serve1(service_cmd='restart', **kwargs):
    if cmd_avail('systemctl'):
        return restart_systemd('postgresql')
    else:
        return sudo('service postgres {service_cmd}'.format(service_cmd=service_cmd))


def _cluster_with_pgpool(conf_location, template_vars=None, *args, **kwargs):
    # conf_location should have pgpool.conf, relative to python package dir
    apt_depends('pgpool2')

    default_tpl_vars = {'PORT': '5433'}
    if not template_vars:
        template_vars = default_tpl_vars
    else:
        for k, v in default_tpl_vars.iteritems():
            if k not in template_vars:
                template_vars[k] = v

    upload_template(conf_location, '/etc/pgpool2/pgpool.conf',
                    mode=640, context=template_vars, use_sudo=True)
    sudo('chown root:postgres /etc/pgpool2/pgpool.conf')
    upload_template(conf_location, '/usr/share/pgpool2/pgpool.conf',
                    mode=644, context=template_vars, use_sudo=True)
    sudo('chown root:root /usr/share/pgpool2/pgpool.conf')
