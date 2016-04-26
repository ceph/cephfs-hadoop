FROM ubuntu:14.04
MAINTAINER Noah Watkins <noahwatkins@gmail.com>

RUN apt-get update && apt-get -y install curl \
  openssh-server maven default-jdk software-properties-common

### fetch and unpack hadoop
RUN curl -O http://www-us.apache.org/dist/hadoop/common/hadoop-2.6.4/hadoop-2.6.4.tar.gz
RUN tar xzvf hadoop-2.6.4.tar.gz

### install cephfs java bindings
RUN wget -q -O- 'https://download.ceph.com/keys/release.asc' | apt-key add -
RUN apt-add-repository 'deb http://download.ceph.com/debian-hammer/ trusty main'
RUN apt-get update && apt-get -y install libcephfs1 libcephfs-dev \
  libcephfs-jni libcephfs-java

### setup password-less ssh to localhost
RUN sed -i 's/UsePAM yes/UsePAM no/g' /etc/ssh/sshd_config
RUN ssh-keygen -f $HOME/.ssh/id_rsa -t rsa -N '' && \
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
  chmod 600 ~/.ssh/authorized_keys
  RUN echo 'Host *\n  StrictHostKeyChecking no' > ~/.ssh/config

ADD entrypoint.sh /entrypoint.sh
ENTRYPOINT [ "/entrypoint.sh" ]
