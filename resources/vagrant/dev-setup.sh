wget http://mirrors.gigenet.com/apache/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
tar -zxvf apache-maven-3.0.5-bin.tar.gz -C /opt/
ln -s /opt/apache-maven-3.0.5/bin/mvn /usr/bin/mvn

echo "make sure to export JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/"

echo "Now building ceph-hadoop !!!!!!!!!!!!!"
cd /ceph-hadoop/

### Use maven to install a custom build version of hadoop with HADOOP-9361 tests. ###
mvn install:install-file -Dfile=resources/hadoop-common-3.0.0-SNAPSHOT-tests.jar -DgroupId=com.rhbd.hcfs
-DartifactId=hadoop-common-latest-tests -Dversion=0.1 -Dpackaging=jar

mvn install:install-file -Dfile=resources/hadoop-common-3.0.0-SNAPSHOT.jar -DgroupId=com.rhbd.hcfs
-DartifactId=hadoop-common-latest -Dversion=0.1 -Dpackaging=jar

mvn clean package
