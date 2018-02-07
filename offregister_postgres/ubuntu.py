from cStringIO import StringIO
from functools import partial

from fabric.api import sudo
from fabric.contrib.files import upload_template
from fabric.operations import put

from offregister_fab_utils.apt import apt_depends
from offregister_fab_utils.fs import cmd_avail
from offregister_fab_utils.ubuntu.systemd import restart_systemd


def install0(version='9.6', username='postgres', dbs=None, users=None,
             extra_deps=tuple(), cluster=False, cluster_conf=None, **kwargs):
    sio = StringIO()
    sio.write('deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main')
    put(sio, '/etc/apt/sources.list.d/pgdg.list', use_sudo=True)
    sudo('wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -')

    apt_depends('postgresql-{version}'.format(version=version),
                'postgresql-contrib-{version}'.format(version=version),
                'postgresql-server-dev-{version}'.format(version=version),
                *extra_deps)
    postgres = partial(sudo, user=username)

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
