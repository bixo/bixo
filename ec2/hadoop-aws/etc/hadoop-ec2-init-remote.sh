#!/usr/bin/env bash

################################################################################
# Script that is run on each EC2 instance on boot. It is passed in the EC2 user
# data, so should not exceed 16K in size.
################################################################################

################################################################################
# Initialize variables
################################################################################

# Slaves are started after the master, and are told its address by sending a
# modified copy of this file which sets the MASTER_HOST variable. 
# A node  knows if it is the master or not by inspecting the security group
# name. If it is the master then it retrieves its address using instance data.
MASTER_HOST=%MASTER_HOST% # Interpolated before being sent to EC2 node
SECURITY_GROUPS=`wget -q -O - http://169.254.169.254/latest/meta-data/security-groups`
IS_MASTER=`echo $SECURITY_GROUPS | awk '{ a = match ($0, "-master$"); if (a) print "true"; else print "false"; }'`
if [ "$IS_MASTER" == "true" ]; then
 MASTER_HOST=`wget -q -O - http://169.254.169.254/latest/meta-data/local-hostname`
fi

# These values get filled in by launch-hadoop-master and launch-hadoop-slaves
# locally to create the version of this file that is set to the remote instance.
# The variables (and other variables in this script) are then set up and used
# when the script is run on the instance to set up various configuration files
# for the master or slave.
AWS_ACCESS_KEY_ID=%AWS_ACCESS_KEY_ID%
AWS_SECRET_ACCESS_KEY=%AWS_SECRET_ACCESS_KEY%
INSTANCE_TYPE=%INSTANCE_TYPE%
EXTRA_CORE_SITE_PROPERTIES="%EXTRA_CORE_SITE_PROPERTIES%"
EXTRA_HDFS_SITE_PROPERTIES="%EXTRA_HDFS_SITE_PROPERTIES%"
EXTRA_MAPRED_SITE_PROPERTIES="%EXTRA_MAPRED_SITE_PROPERTIES%"

HADOOP_HOME=`ls -d /usr/local/hadoop-*`

# For m1.small slaves, we only get one virtual core (1 EC2 Compute Unit).
# For m1.large slaves, we actually have a second drive which can share the HDFS load.
#
if [ "$INSTANCE_TYPE" == "m1.small" ]; then
 NUM_SLAVE_CORES=1
 HDFS_DATA_DIR="/mnt/hadoop/dfs/data"
else
 NUM_SLAVE_CORES=2
 HDFS_DATA_DIR="/mnt/hadoop/dfs/data,/mnt2/hadoop/dfs/data"
 mkdir -p /mnt2/hadoop
fi

################################################################################
# Hadoop configuration
# Modify this section to customize your Hadoop cluster.
################################################################################

cat > $HADOOP_HOME/conf/core-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>

<!--- global properties -->

<property>
  <name>hadoop.tmp.dir</name>
  <value>/mnt/hadoop</value>
  <description>A base for other temporary directories.
  
  AWS EC2 instances have a large /mnt drive for log files, etc.
  </description>
</property>

<!-- i/o properties -->

<!-- file system properties -->

<property>
  <name>fs.default.name</name>
  <value>hdfs://$MASTER_HOST:50001</value>
  <description>The name of the default file system.  A URI whose
  scheme and authority determine the FileSystem implementation.  The
  uri's scheme determines the config property (fs.SCHEME.impl) naming
  the FileSystem implementation class.  The uri's authority is used to
  determine the host, port, etc. for a filesystem.
  
  While running in EC2, we want to default to HDFS.
  </description>
</property>

<!-- AWS EC2-specific properties -->

<property>
  <name>fs.s3n.awsAccessKeyId</name>
  <value>$AWS_ACCESS_KEY_ID</value>
</property>

<property>
  <name>fs.s3n.awsSecretAccessKey</name>
  <value>$AWS_SECRET_ACCESS_KEY</value>
</property>

<property>
  <name>fs.sqs.awsAccessKeyId</name>
  <value>$AWS_ACCESS_KEY_ID</value>
</property>

<property>
  <name>fs.sqs.awsSecretAccessKey</name>
  <value>$AWS_SECRET_ACCESS_KEY</value>
</property>

$EXTRA_CORE_SITE_PROPERTIES

</configuration>
EOF

cat > $HADOOP_HOME/conf/hdfs-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>

<property>
  <name>dfs.permissions</name>
  <value>false</value>
  <description>
    If "true", enable permission checking in HDFS.
    If "false", permission checking is turned off,
    but all other behavior is unchanged.
    Switching from one parameter value to the other does not change the mode,
    owner or group of files or directories.
    
    TODO CSc Figure out why AWS wants this set to false.
  </description>
</property>

<property>
  <name>dfs.data.dir</name>
  <value>$HDFS_DATA_DIR</value>
  <description>Determines where on the local filesystem an DFS data node
  should store its blocks.  If this is a comma-delimited
  list of directories, then data will be stored in all named
  directories, typically on different devices.
  Directories that do not exist are ignored.
  
  The m1.large instances have two drives, so we set this up above dependent on $INSTANCE_TYPE.
  </description>
</property>

$EXTRA_HDFS_SITE_PROPERTIES

</configuration>
EOF

# TODO CSc Should we install and use an LZO compression codec?
#
cat > $HADOOP_HOME/conf/mapred-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>

<!-- i/o properties -->

<property>
  <name>mapred.job.tracker</name>
  <value>hdfs://$MASTER_HOST:50002</value>
  <description>The host and port that the MapReduce job tracker runs
  at.  If "local", then jobs are run in-process as a single map
  and reduce task.
  
  Here we specify the AWS EC2 job tracker host and port.
  </description>
</property>

<property>
  <name>mapred.task.timeout</name>
  <value>1200000</value>
  <description>The number of milliseconds before a task will be
  terminated if it neither reads an input, writes an output, nor
  updates its status string.
  
  Bump to 20 minutes (was 10m, or 600K ms)
  </description>
</property>

<property>
  <name>mapred.tasktracker.map.tasks.maximum</name>
  <value>$NUM_SLAVE_CORES</value>
  <description>The maximum number of map tasks that will be run
  simultaneously by a task tracker.
  
  For m1.small slaves, we only get one virtual core (1 EC2 Compute Unit),
  so we shouldn't be running two map tasks on the same slave simultaneously.
  </description>
</property>
<property>
  <name>mapred.tasktracker.reduce.tasks.maximum</name>
  <value>$NUM_SLAVE_CORES</value>
  <description>The maximum number of reduce tasks that will be run
  simultaneously by a task tracker.
  
  For m1.small slaves, we only get one virtual core (1 EC2 Compute Unit),
  so we shouldn't be running two reduce tasks on the same slave simultaneously.
  </description>
</property>

<property>
  <name>tasktracker.http.threads</name>
  <value>80</value>
  <description>The number of worker threads that for the http server. This is
               used for map output fetching
               
  Double number of threads per AWS recommendation.
  </description>
</property>

<property>
  <name>mapred.output.compress</name>
  <value>true</value>
  <description>Should the job outputs be compressed?
  
  Compress them by default, though jobs can override this.
  </description>
</property>

<property>
  <name>mapred.output.compression.type</name>
  <value>BLOCK</value>
  <description>If the job outputs are to compressed as SequenceFiles, how should
               they be compressed? Should be one of NONE, RECORD or BLOCK.
               
  Compressing on a record basis is probably better for Cascading jobs.
  </description>
</property>

<property>
  <name>mapred.compress.map.output</name>
  <value>true</value>
  <description>Should the outputs of the maps be compressed before being
               sent across the network. Uses SequenceFile compression.
               
  The maps often generate huge files outside DFS, so it's better to compress them.
  </description>
</property>

$EXTRA_MAPRED_SITE_PROPERTIES

</configuration>
EOF

# Configure Hadoop for Ganglia
# overwrite hadoop-metrics.properties
cat > $HADOOP_HOME/conf/hadoop-metrics.properties <<EOF

# Ganglia
# we push to the master gmond so hostnames show up properly
dfs.class=org.apache.hadoop.metrics.ganglia.GangliaContext
dfs.period=10
dfs.servers=$MASTER_HOST:8649

mapred.class=org.apache.hadoop.metrics.ganglia.GangliaContext
mapred.period=10
mapred.servers=$MASTER_HOST:8649

jvm.class=org.apache.hadoop.metrics.ganglia.GangliaContext
jvm.period=10
jvm.servers=$MASTER_HOST:8649
EOF

################################################################################
# s3cmd configuration
################################################################################

cat > /root/.s3cfg <<EOF
[default]
access_key = $AWS_ACCESS_KEY_ID
acl_public = False
bucket_location = US
debug_syncmatch = False
default_mime_type = binary/octet-stream
delete_removed = False
dry_run = False
encrypt = False
force = False
gpg_command = /usr/bin/gpg
gpg_decrypt = %(gpg_command)s -d --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
gpg_encrypt = %(gpg_command)s -c --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
gpg_passphrase = 
guess_mime_type = False
host_base = s3.amazonaws.com
host_bucket = %(bucket)s.s3.amazonaws.com
human_readable_sizes = False
preserve_attrs = True
proxy_host = 
proxy_port = 0
recv_chunk = 4096
secret_key = $AWS_SECRET_ACCESS_KEY
send_chunk = 4096
simpledb_host = sdb.amazonaws.com
use_https = False
verbosity = WARNING
EOF

################################################################################
# Start services
################################################################################

[ ! -f /etc/hosts ] &&  echo "127.0.0.1 localhost" > /etc/hosts

mkdir -p /mnt/hadoop/logs

# not set on boot
export USER="root"

if [ "$IS_MASTER" == "false" ]; then
  # We want to constrain the kernel stack size used for slaves, since we
  # fire up a bunch of threads. We won't need this once Hadoop supports specifying
  # stack size as part of the ulimit settings in the job conf. But for now this
  # needs to be set up before Hadoop starts running.

  cat > /etc/security/limits.conf <<EOF
root	soft	nofile	65535
root	hard	nofile	65535
root	soft	stack	256
root	hard	stack	256
EOF
fi

if [ "$IS_MASTER" == "true" ]; then
  # MASTER
  # Prep Ganglia
  sed -i -e "s|\( *mcast_join *=.*\)|#\1|" \
         -e "s|\( *bind *=.*\)|#\1|" \
         -e "s|\( *mute *=.*\)|  mute = yes|" \
         -e "s|\( *location *=.*\)|  location = \"master-node\"|" \
         /etc/gmond.conf
  mkdir -p /mnt/ganglia/rrds
  chown -R ganglia:ganglia /mnt/ganglia/rrds
  rm -rf /var/lib/ganglia; cd /var/lib; ln -s /mnt/ganglia ganglia; cd
  service gmond start
  service gmetad start
  apachectl start

  # Hadoop
  # only format on first boot
  [ ! -e /mnt/hadoop/dfs ] && "$HADOOP_HOME"/bin/hadoop namenode -format

  "$HADOOP_HOME"/bin/hadoop-daemon.sh start namenode
  "$HADOOP_HOME"/bin/hadoop-daemon.sh start jobtracker
else
  # SLAVE
  # Prep Ganglia
  sed -i -e "s|\( *mcast_join *=.*\)|#\1|" \
         -e "s|\( *bind *=.*\)|#\1|" \
         -e "s|\(udp_send_channel {\)|\1\n  host=$MASTER_HOST|" \
         /etc/gmond.conf
  service gmond start

  # Hadoop
  "$HADOOP_HOME"/bin/hadoop-daemon.sh start datanode
  
  # Set limits for tasktracker - use roughly half of max (65535) since
  # we'll typically have two trackers per server.
  ulimit -n 30000
  ulimit -s 256
  "$HADOOP_HOME"/bin/hadoop-daemon.sh start tasktracker
fi

# Values for nscd are:
# 	positive-time-to-live	hosts		86400 (1 day)
# 	negative-time-to-live	hosts		3600 (1 hour)
# 	suggested-size			hosts		331 (prime # a bit bigger than 211)
# 	max-db-size				hosts		134217728 (128MB)

if [ "$IS_MASTER" == "false" ]; then
  # NSCD
  sed -r -i -e "s|(^\t*positive-time-to-live\t*hosts\t*)[0-9]+.*$|\186400|" \
            -e "s|(^\t*negative-time-to-live\t*hosts\t*)[0-9]+.*$|\13600|" \
            -e "s|(^\t*suggested-size\t*hosts\t*)[0-9]+.*$|\1331|" \
            -e "s|(^\t*max-db-size\t*hosts\t*)[0-9]+.*$|\1134217728|" \
         /etc/nscd.conf

  service nscd start
fi

# Run this script on next boot
# 
# TODO CSc Re-enable this file removal once testing is complete!
#
#rm -f /var/ec2/ec2-run-user-data.*
