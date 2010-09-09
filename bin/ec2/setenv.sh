#!/bin/bash

# This bash script must be run before any of the other Amazon EC2 tools,
# so that the shell variables and other files they depend on will be set up.
# This script is shared by multiple projects, so don't please don't make
# any project-specific changes.

echo "initializing environment"

# The directory containing this setenv.sh script.
#
# TODO CSc We should capture the project-specific current working directory
# from which the user will be executing hadoop-ec2 commands for the project.
# We should place $HADOOP_EC2_ENV in that directory, but expect $HADOOP_AWS_HOME
# and $EC2_HOME to be inside $BIXO_BASEDIR=$BIXO_HOME/ec2/ (along with this
# setenv.sh script). We could set up $BIXO_HOME here if it's not already set,
# but how does the user run this script from some other project directory?
# I guess each project has an ec2/ subdirectory with its own setenv.sh script
# that just executes $BIXO_HOME/ec2/setenv.sh to run this script.
# All the project-specific configuration files would go in the project's ec2
# directory. It might feel more convenient to run setenv.sh and hadoop-ec2
# from the top-level of the project, but it's also nice to collect all of the
# project's EC2 junk within a single directory.
#
PROJECT_BASEDIR=`pwd`
BASEDIR=`dirname "$0"`
BASEDIR=`cd "$BASEDIR"; pwd`
cd $PROJECT_BASEDIR

# The $HADOOP_AWS_HOME folder contains a number of bash scripts that make
# using the Amazon EC2 API Tools (see below) easier by setting up shell
# variables and configuration files and providing a higher-level interface
# for launching and keeping track of clusters.
#
export HADOOP_AWS_HOME=$BASEDIR/hadoop-aws

# The $EC2_HOME folder contains the standard set of Elastic Computing Cloud
# command-line tools from Amazon Web Services (aka Amazon EC2 API Tools).
# These can be downloaded from S3 at http://tinyurl.com/aws-ec2-api-tools
# The documentation for these tools is at:
# http://docs.amazonwebservices.com/AWSEC2/latest/CommandLineReference/
#
if [ -z "$EC2_HOME" ]; then
	export EC2_HOME=$BASEDIR/support/ec2-api-tools-1.3-53907
fi

# Add the $HADOOP_AWS_HOME high-level scripts and the $EC2_HOME standard
# Amazon EC2 API Tools to $PATH so it's easy to execute them.
#
export PATH=$HADOOP_AWS_HOME/bin:$EC2_HOME/bin:$PATH

# You must put a file named .local.awskey-path into $BASEDIR.
# It should contain the path to your EC2 keys directory.  These keys are used
# by the EC2 API Tools to communicate securely with the AWS servers.
#
if [ -e "$BASEDIR/.local.awskey-path" ]; then
    export AWS_KEYDIR=`cat $BASEDIR/.local.awskey-path`
else
    echo "Error: $BASEDIR/.local.awskey-path does not exist!"
    return
fi

# It's possible to have multiple cluster profiles, each with its own set of shell
# variables. The profile name may be specified as the argument of this script.
# 
CT_PROFILE=${1:+.$1}

# The hadoop_aws/etc/ folder contains a template for setting up all of the common
# shell variables, but the account-specific ones are left blank. The sed command
# below maps the template into a hidden file that lives in this directory and is
# executed by this script to define all of the shell variables.  You should have
# such a template for the default cluster profile (and any other cluster profiles
# you define).
# 
CT_BASE=$HADOOP_AWS_HOME/etc/hadoop-ec2-env${CT_PROFILE}.sh
[ ! -f $CT_BASE ] && echo "Error: $CT_BASE not found!" && return
export HADOOP_EC2_ENV=$PROJECT_BASEDIR/.local${CT_PROFILE}.hadoop-ec2-env.sh

# Collect security information from the contents of the EC2 keys directory
# so that it can be used by the EC2 API Tools to communicate securely with
# the AWS servers.
#
export EC2_PRIVATE_KEY=`find $AWS_KEYDIR  -path '*/pk*.pem'`
export EC2_CERT=`find $AWS_KEYDIR -path '*/cert*.pem'`
export ID_RSA_KEY_NAME=`find $AWS_KEYDIR -path '*/id_rsa*' | sed -e "s|^.*id_rsa-\(.*\)$|\1|"`

# used to create images and access s3

# Fill in the values of the account-specific shell variables using the contents of
# files expected to live inside the EC2 keys directory.
#
export AWS_ACCOUNT_ID_FILE=$AWS_KEYDIR/accountid
export AWS_ACCESS_KEY_FILE=$AWS_KEYDIR/accesskey
export AWS_SECRET_KEY_FILE=$AWS_KEYDIR/secretkey

export AWS_ACCOUNT_ID=`cat $AWS_ACCOUNT_ID_FILE`
export AWS_ACCESS_KEY_ID=`cat $AWS_ACCESS_KEY_FILE`
export AWS_SECRET_ACCESS_KEY=`cat $AWS_SECRET_KEY_FILE`

# Map the template for setting up all the AWS shell variables into a complete file,
# using the values of these account-specific variables to fill in the blanks.
# Save it in a hidden file inside this directory (where all of these variables can
# be easily examined by a human).
#
sed -e "s|^AWS_ACCOUNT_ID=|AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID}|" \
    -e "s|^AWS_ACCESS_KEY_ID=|AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}|" \
    -e "s|^AWS_SECRET_ACCESS_KEY=|AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}|" \
    -e "s|^KEY_NAME=|KEY_NAME=${ID_RSA_KEY_NAME}|" \
      $CT_BASE > $HADOOP_EC2_ENV

# Execute the resulting file, thereby defining all of the AWS shell variables.
#
. $HADOOP_EC2_ENV

# Let the user know where the hidden file containing the settings is, remind them
# that we've set up some options providing security parameters for doing SSH into EC2,
# (and that they better have access to the single S3 bucket containing all of the AMIs).
#
echo 'using HADOOP_EC2_ENV: '$HADOOP_EC2_ENV
echo 'use "ssh $SSH_OPTS ..." for standard ssh commands against EC2'
echo 'the current ACCESS_KEY must have access to the S3 bucket named: '$S3_BUCKET
echo ""
