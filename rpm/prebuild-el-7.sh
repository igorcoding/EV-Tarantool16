#! /usr/bin/env bash

echo "Installing Tarantool version=${VAR_TARANTOOL}"

if [ -z "${VAR_TARANTOOL}" ]; then
	VAR_TARANTOOL="1.9"
fi

# Clean up yum cache
sudo yum clean all

# Enable EPEL repository
sudo yum -y install http://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
sudo sed 's/enabled=.*/enabled=1/g' -i /etc/yum.repos.d/epel.repo

# Add Tarantool repository
sudo rm -f /etc/yum.repos.d/*tarantool*.repo
sudo tee /etc/yum.repos.d/tarantool_${VAR_TARANTOOL}.repo <<- EOF
[tarantool]
name=EnterpriseLinux-7 - Tarantool
baseurl=http://download.tarantool.org/tarantool/${VAR_TARANTOOL}/el/7/x86_64/
gpgkey=http://download.tarantool.org/tarantool/${VAR_TARANTOOL}/gpgkey
repo_gpgcheck=1
gpgcheck=0
enabled=1

[tarantool-source]
name=EnterpriseLinux-7 - Tarantool Sources
baseurl=http://download.tarantool.org/tarantool/${VAR_TARANTOOL}/el/7/SRPMS
gpgkey=http://download.tarantool.org/tarantool/${VAR_TARANTOOL}/gpgkey
repo_gpgcheck=1
gpgcheck=0
EOF

# Update metadata
sudo yum makecache -y --disablerepo='*' --enablerepo='tarantool' --enablerepo='epel'

# Install Tarantool
sudo yum -y install tarantool
