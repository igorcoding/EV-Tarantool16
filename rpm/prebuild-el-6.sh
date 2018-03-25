#! /usr/bin/env bash

echo "Installing Tarantool"

# Clean up yum cache
sudo yum clean all

# Enable EPEL repository
sudo yum -y install http://dl.fedoraproject.org/pub/epel/epel-release-latest-6.noarch.rpm
sudo sed 's/enabled=.*/enabled=1/g' -i /etc/yum.repos.d/epel.repo

# Add Tarantool repository
sudo rm -f /etc/yum.repos.d/*tarantool*.repo
sudo tee /etc/yum.repos.d/tarantool_1_9.repo <<- EOF
[tarantool_1_9]
name=EnterpriseLinux-6 - Tarantool
baseurl=http://download.tarantool.org/tarantool/1.9/el/6/x86_64/
gpgkey=http://download.tarantool.org/tarantool/1.9/gpgkey
repo_gpgcheck=1
gpgcheck=0
enabled=1

[tarantool_1_9-source]
name=EnterpriseLinux-6 - Tarantool Sources
baseurl=http://download.tarantool.org/tarantool/1.9/el/6/SRPMS
gpgkey=http://download.tarantool.org/tarantool/1.9/gpgkey
repo_gpgcheck=1
gpgcheck=0
EOF

# Update metadata
sudo yum makecache -y --disablerepo='*' --enablerepo='tarantool_1_9' --enablerepo='epel'

# Install Tarantool
sudo yum -y install tarantool
