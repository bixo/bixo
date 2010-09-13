# Set environment variables for running Hadoop on Amazon EC2 here. All are required.

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The current cluster 'profile', default is null
HADOOP_EC2_PROFILE=

# Your Amazon Account Number.
AWS_ACCOUNT_ID=

# Your Amazon AWS access key.
AWS_ACCESS_KEY_ID=

# Your Amazon AWS secret access key.
AWS_SECRET_ACCESS_KEY=

# Location of EC2 keys.
# The default setting is probably OK if you set up EC2 following the Amazon Getting Started guide.
EC2_KEYDIR=`dirname "$EC2_PRIVATE_KEY"`

# The EC2 key name used to launch instances.
# The default is the value used in the Amazon Getting Started guide.
KEY_NAME=

# Where your EC2 private key is stored (created when following the Amazon Getting Started guide).
# You need to change this if you don't store this with your other EC2 keys.
PRIVATE_KEY_PATH=`echo "$EC2_KEYDIR"/"id_rsa-$KEY_NAME"`

# SSH options used when connecting to EC2 instances.
SSH_OPTS=`echo -i "$PRIVATE_KEY_PATH" -o StrictHostKeyChecking=no -o ServerAliveInterval=30`

# The version of Hadoop to use.
#
# TODO CSc It would be nice to be able to make this project-specific without overriding
# this entire file, but it's not that important.
#
HADOOP_VERSION=0.20.2

# The Amazon S3 bucket where the Hadoop AMI is stored.
# The default value is for public images, so can be left if you are using running a
# public image. Change this value only if you are creating your own (private) AMI
# so you can store it in a bucket you own.
#S3_BUCKET=cloudera-ec2-hadoop-images
#S3_BUCKET=hadoop-images
S3_BUCKET=bixolabs-public-amis

# Enable public access to JobTracker and TaskTracker web interfaces
ENABLE_WEB_PORTS=true

# TODO CSc I renamed $USER_DATA_FILE to be $USER_DATA_FILE_TEMPLATE, and put the whole
# path into this variable. I changed launch-master and launch-slaves to use the new
# variable, but I wonder if any other EC2 scripts use it. Is it worth just writing
# out its definition here?

# The EC2 instance type to use if $INSTANCE_TYPE is not set
# $INSTANCE_TYPE can be set via an optional parameter to
# $HADOOP_AWS_HOME/bin/[hadoop-ec2|launch-hadoop-[cluster|master|slaves]] scripts
DEFAULT_INSTANCE_TYPE="m1.small"
#DEFAULT_INSTANCE_TYPE="m1.large"
#DEFAULT_INSTANCE_TYPE="m1.xlarge"
#DEFAULT_INSTANCE_TYPE="m1.medium"
#DEFAULT_INSTANCE_TYPE="m1.xlarge"

# The EC2 group master name. $CLUSTER is set via parameter passed to
# $HADOOP_AWS_HOME/bin/[hadoop-ec2|launch-hadoop-[cluster|master|slaves]] scripts
CLUSTER_MASTER=$CLUSTER-master

# Cached values for a given cluster
#
# TODO CSc Fix these file names (i.e., "hadooop" => "hadoop").
#
MASTER_PRIVATE_IP_PATH=~/.hadooop-private-$CLUSTER_MASTER
MASTER_IP_PATH=~/.hadooop-$CLUSTER_MASTER
MASTER_ZONE_PATH=~/.hadooop-zone-$CLUSTER_MASTER

#
# NOTE: The following variables are only used when *creating* a new AMI.
#

# The version number of the installed JDK.
JAVA_VERSION=1.6.0_10

# SUPPORTED_ARCHITECTURES = ['i386', 'x86_64']
# The download URL for the Sun JDK. Visit http://java.sun.com/javase/downloads/index.jsp and get the URL for the "Linux self-extracting file".
if [ "$INSTANCE_TYPE" == "m1.small" -o "$INSTANCE_TYPE" == "c1.medium" ]; then
  ARCH='i386'
  BASE_AMI_IMAGE="ami-48b54021"  # bixolabs-public-amis/bixolabs-hadoop-0.20.2-i386.manifest.xml
  JAVA_BINARY_URL=''
else
  ARCH='x86_64'
  BASE_AMI_IMAGE="ami-ba55bfd3"  # bixolabs-public-amis/bixolabs-hadoop-0.20.2-x86_64.manifest.xml
  JAVA_BINARY_URL=''
fi

if [ "$INSTANCE_TYPE" == "c1.medium" ]; then
  # AMI_KERNEL=aki-9b00e5f2 # ec2-public-images/vmlinuz-2.6.18-xenU-ec2-v1.0.i386.aki.manifest.xml
  AMI_KERNEL=aki-a71cf9ce # ec2-public-images/ec2-vmlinuz-2.6.21.7-2.fc8xen.i386.manifest.xml
fi

if [ "$INSTANCE_TYPE" == "c1.xlarge" ]; then
  # AMI_KERNEL=aki-9800e5f1 # ec2-public-images/vmlinuz-2.6.18-xenU-ec2-v1.0.x86_64.aki.manifest.xml
  AMI_KERNEL=aki-b51cf9dc # ec2-public-images/ec2-vmlinuz-2.6.21.7-2.fc8xen.x86_64.manifest.xml
fi

if [ "$AMI_KERNEL" != "" ]; then
  KERNEL_ARG="--kernel ${AMI_KERNEL}"
fi
