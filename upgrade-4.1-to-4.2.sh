#!/bin/bash

# Usage: upgrade-4.1-to-4.2.sh <tarball> <prefix>
#
# Prefix is the prefix where the tarball was extracted. Defaults to /opt
#

GROUP=sonar
USER=sonarw
TARBALL=${1:-/home/ec2-user/jsonar-4.2.a.tar}
VERSION=$(echo $(basename ${TARBALL}) | sed -e 's/jsonar-\(.*\).tar.*/\1/')
PREFIX=${2:-/opt}

set -ex
exec > >(tee /var/log/sonar-4.2-upgrade.log | logger -t sonar-4.2 ) 2>&1

list-units() {
    systemctl list-units --full --all --plain --no-legend  \
              'sonar*' 'gateway-*'                         \
        | awk '{print $1}'
}

list-unit-files() {
    systemctl list-unit-files --full --all --plain --no-legend  \
              'sonar*' 'gateway-*'                              \
        | awk '{print $1}'
}

clean-old-services() {
    echo -n "Stopping services..."
    list-units | xargs systemctl stop || true
    list-units | xargs systemctl disable || true
    for file in $(list-unit-files); do
        mv /usr/lib/systemd/system/${file} /usr/lib/systemd/system/${file}_DISABLED_BY_4.1_TO_4.2_UPGRADE_SCRIPT
    done
    systemctl daemon-reload
    echo "done"
}


create-users() {
    echo -n "Creating users..."
    if ! getent group ${GROUP} >/dev/null; then
        groupadd --system ${GROUP}
    fi

    if ! getent passwd ${USER} >/dev/null; then
        useradd --system --comment "jsonar system user" --gid ${GROUP} ${USER}
    fi
    echo "done"
}

cleanup() {
    echo -n "Cleaning up..."
    rm -rf ${PREFIX}/jsonar
    rm -f /etc/sysconfig/jsonar
    echo "done"
}

extract() {
    local tarball=$1
    local prefix=$2

    echo -n "Extracting tarball..."
    tar axf ${tarball} --directory ${prefix}
    echo done
}

chown-files() {
    echo -n "Setting file ownership..."
    chown -R ${USER}:${GROUP} ${PREFIX}/jsonar
    echo done
}

generate-env() {
    echo "Generating environment"
    ${PREFIX}/jsonar/apps/${VERSION}/bin/python3 -m jsonar | tee /etc/sysconfig/jsonar
}

copy-config-dir() {
    local src=$1

    cp -a ${src} ${JSONAR_LOCALDIR}/
}

copy-optional-config-file() {
    local src=$1
    local dst=$2

    if [[ -f ${src} ]]; then
        copy-config-file ${src} ${dst}
    fi
}
copy-config-file() {
    local src=$1
    local dst=${JSONAR_LOCALDIR}/$2

    mkdir -p ${dst}
    cp ${src} ${dst}
}

rename-config-file() {
    local src=${JSONAR_LOCALDIR}/$1
    local dst=$2

    local dir=$(dirname ${src})
    mv ${src} ${dir}/${dst}
}

copy-config-files() {
    echo -n "Copying config files..."
    copy-config-dir /etc/sonar/ssl
    copy-config-dir /etc/sonar/credentials
    copy-config-dir /etc/sonar/gateway
    copy-config-file /etc/default/sonarg sonarg
    rename-config-file sonarg/sonarg default
    copy-config-file /etc/sonar/sonard.conf sonarw
    copy-config-file /etc/sonar/sonargd.conf sonargd
    copy-optional-config-file /etc/sonar/ssh-config sonargd
    copy-config-file /opt/sonarfinder/sonarFinder/config.properties sonarfinder
    copy-config-file /opt/sonarfinder/sonarFinder/dispatcher.conf dispatcher
    copy-config-file /etc/sonar/sonares.conf sonares
    copy-config-file /etc/sonar/sonarremote.conf sonarremote
    copy-config-file /etc/sonar/sonarg_azure_eventhub.conf sonarg_azure_eventhub
    copy-config-file /etc/sonar/sonarg_azure_eventhub.log4j2.yaml sonarg_azure_eventhub
    copy-config-file /etc/sonar/sage.conf sage
    copy-config-file /etc/sonar/kibana.yml sonark
    rename-config-file sonark/kibana.yml sonark.yml
    copy-optional-config-file /etc/sonar/sonarsql.conf sonarsql
    echo done
}

modify-config-files() {
    echo -n "Modifying config files..."
    for file in $(grep -rl /etc/sonar ${JSONAR_LOCALDIR}); do
        sed -i 's@/etc/sonar/@${JSONAR_LOCALDIR}/@;
                s@/var/log/sonar@${JSONAR_LOGDIR}/sonar@;
                s@/var/lib/sonarw@${JSONAR_DATADIR}/sonarw@' $file
    done
    echo done
}

run-sonarg-setup() {
    local bindir=${PREFIX}/jsonar/apps/${VERSION}/bin
    ${bindir}/python3 ${bindir}/sonarg-setup
}

copy-sonarw-data() {
    echo -n "Copying sonarw data..."
    eval $(cat /etc/sysconfig/sonard)
    cp -a ${SONAR_HOME} ${JSONAR_DATADIR}
    echo done
}

clean-old-services
create-users
cleanup
extract ${TARBALL} ${PREFIX}
generate-env
export $(cat /etc/sysconfig/jsonar)
mkdir -p ${JSONAR_DATADIR} ${JSONAR_LOGDIR}/sonargd ${JSONAR_LOCALDIR}
copy-sonarw-data
copy-config-files
modify-config-files
chown-files
run-sonarg-setup