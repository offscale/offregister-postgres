from functools import partial
from fabric.api import sudo
from fabric.contrib.files import upload_template
from offregister_fab_utils.apt import apt_depends


def install(version='9.3', username='postgres', dbs=None, users=None,
            extra_deps=tuple(), cluster=False, cluster_conf=None):
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
        cluster_with_pgpool2(cluster_conf)

    return {'dbs': map(require_db, dbs), 'users': map(require_user, users)}


def serve(service_cmd='restart'):
    sudo('service postgres {service_cmd}'.format(service_cmd=service_cmd))


def cluster_with_pgpool2(conf_location, template_vars=None):
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
