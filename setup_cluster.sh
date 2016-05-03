#!/bin/bash

# base multi-node cluster setup off joeljacobson template
# using vagrant and ansible
git clone https://github.com/joeljacobson/vagrant-ansible-cassandra.git dse-vagrant

# launch the DSE cluster
cd dse-vagrant
vagrant up

# Nodes will be running on: 192.168.56.11, 192.168.56.12, 192.168.56.13
# Opscenter will be running on: 192.168.56.14:8888
