CephFS Hadoop Plugin!
=====================

## In a hurry ? ## 

- Install virtualbox and vagrant.
- Make sure they are working correctly.

Then just run:

- cd ./resources/vagrant
- vagrant up

## Wow ! How did you do that? Vagrant ## 

This repository contains the source code for the Hadoop FileSystem (HCFS) implementation on  Ceph.

In addition, for developers, it includes a Vagrant recipe for spinning up a Ceph 1 node cluster to test the plugin.

The vagrant recipe

 - installs ceph-deploy, ceph, ceph-fuse, etc..
 - installs the ceph java bindings
 - configures and sets up a single node cluster
 - creates a fuse mount in /mnt/ceph
 - installs maven
 - creates a shared directory for development (/ceph-hadoop)
 - creates a shared directory for vagrant setup (/vagrant) 
 - installs custom HCFS jars for HADOOP-9361
 - finally runs the entire build, creates the jar, and runs unit tests.

## Learning the details ##

To grok the details, just check out the Vagrantfile.  In that file, we call 4 scripts (config.vm.provision).
The java steps are summarized by the maven download and `mvn clean package` step.

## Publishing , deployment , and continuous integration ##

This is all TBD.  For now, we manually publish this jar to maven central, see pom.xml for details. 
