from fabric.api import sudo
from fabric.contrib.files import upload_template, append
from fabric.operations import run
from offregister_fab_utils.apt import apt_depends
from offregister_fab_utils.fs import cmd_avail
from offregister_fab_utils.ubuntu.systemd import restart_systemd

from offregister_postgres.utils import setup_users


def install0(version='12',
             extra_deps=tuple(), **kwargs):
    ver = sudo("dpkg-query --showformat='${Version}'" +
               ' --show postgresql-{version}'.format(version=version), warn_only=True)
    apt_depends('sysstat')
    if ver.failed or not ver.startswith(version):
        dist = run('lsb_release -cs')
        append('/etc/apt/sources.list.d/pgdg.list',
               'deb http://apt.postgresql.org/pub/repos/apt/ {dist}-pgdg main'.format(dist=dist),
               use_sudo=True)
        sudo('wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -')

        apt_depends('postgresql-{version}'.format(version=version),
                    'postgresql-contrib-{version}'.format(version=version),
                    'postgresql-server-dev-{version}'.format(version=version),
                    *extra_deps)


def setup_users1(username='postgres', dbs=None, users=None, cluster=False, cluster_conf=None,
                 superuser=False, *args, **kwargs):
    return setup_users(username=username, dbs=dbs, users=users, cluster=cluster, superuser=superuser,
                       kwargs=kwargs)


def serve2(service_cmd='restart', **kwargs):
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
