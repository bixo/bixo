#!/bin/bash

# This bash script must be sourced before any of the other tools in this folder
# can be used.  It sets up environment variables and other files that these tools
# depend on.
#
# Note: This script is shared by multiple projects, so please don't make any
# project-specific changes. Instead, edit the project-specific setenv.sh folder
# within your project's ec2/ directory (which should source this file).

# TODO CSc Leverage what Whirl is doing for EC2 cluster setup?

echo "Initializing EC2 environment..."

# The folder containing this setenv.sh script.
#
export EC2_BASEDIR=`pwd`
if [ ! -e "$EC2_BASEDIR/setenv.sh" ]; then
    echo "Error: You must execute this script from within the ec2 directory."
    return
fi
if [ -z "$PROJECT_EC2_BASEDIR" ]; then
    export PROJECT_EC2_BASEDIR="$EC2_BASEDIR"
fi
echo 'EC2_BASEDIR='$EC2_BASEDIR
echo 'PROJECT_EC2_BASEDIR='$PROJECT_EC2_BASEDIR

# The $HADOOP_AWS_HOME folder contains a number of bash scripts that make
# using the Amazon EC2 API Tools (see below) easier by setting up shell
# variables and configuration files and providing a higher-level interface
# for launching and keeping track of clusters.
#
if [ -z "$HADOOP_AWS_HOME" ]; then
    if [ ! -d "$EC2_BASEDIR/hadoop-aws" ]; then
        echo "Error: $EC2_BASEDIR/hadoop-aws/ directory does not exist!"
        return
    fi
    export HADOOP_AWS_HOME="$EC2_BASEDIR/hadoop-aws"
elif [ ! -d "$HADOOP_AWS_HOME" ]; then
    echo "Error: $HADOOP_AWS_HOME/ directory does not exist!"
    return
fi
#echo 'HADOOP_AWS_HOME='$HADOOP_AWS_HOME

# The $EC2_HOME folder contains the standard set of Elastic Computing Cloud
# command-line tools from Amazon Web Services (aka Amazon EC2 API Tools).
#
# Note: These tools can be downloaded from S3 at:
# http://tinyurl.com/aws-ec2-api-tools.
#
# Note: The documentation for these tools is at:
# http://docs.amazonwebservices.com/AWSEC2/latest/CommandLineReference/
#
if [ -z "$EC2_HOME" ]; then
    EC2_API_TOOLS_VRESION="1.5.2.5"
    if [ ! -d "$EC2_BASEDIR/support/ec2-api-tools-$EC2_API_TOOLS_VRESION" ]; then
        echo "Error: $EC2_BASEDIR/support/ec2-api-tools-$EC2_API_TOOLS_VRESION/ directory does not exist!"
        return
    fi
    export EC2_HOME="$EC2_BASEDIR/support/ec2-api-tools-$EC2_API_TOOLS_VRESION"
elif [ ! -d "$EC2_HOME" ]; then
    echo "Error: $EC2_HOME/ directory does not exist!"
    return
fi
#echo 'EC2_HOME='$EC2_HOME

# Add the $HADOOP_AWS_HOME high-level scripts and the $EC2_HOME standard
# Amazon EC2 API Tools to $PATH so it's easy to execute them.
#
export PATH=$HADOOP_AWS_HOME/bin:$EC2_HOME/bin:$PATH

# You must put a file named .local.awskey-path into $EC2_BASEDIR.
# This file should contain the path to a directory containing all of the
# Amazon EC2 authentication files associated with a single AWS account.
#
# Note: As this is an inherently user-specific location, keeping the path
# in a hidden file with the prefix ".local." makes it easier to prevent the
# source control system from incorporating this information
# (e.g., by adding .local.* to your .gitignore file).
#
# Note: A project-specific location (which would still be user-specific)
# can be substituted by putting a project-specific .local.awskey-path file
# into $PROJECT_EC2_BASEDIR.
#
if [ -f "$PROJECT_EC2_BASEDIR/.local.awskey-path" ]; then
    export AWS_KEYDIR=`cat $PROJECT_EC2_BASEDIR/.local.awskey-path`
elif [ -f "$EC2_BASEDIR/.local.awskey-path" ]; then
    export AWS_KEYDIR=`cat $EC2_BASEDIR/.local.awskey-path`
else
    echo "Error: $EC2_BASEDIR/.local.awskey-path does not exist!"
    return
fi
#echo 'AWS_KEYDIR='$AWS_KEYDIR

# It's possible to have multiple cluster profiles, each with its own set of shell
# variables. The profile name is specified as the argument of this script.
# Here we prepend a "." to it for qualifying file names.
# 
CT_PROFILE_NAME=${1:+.$1}

# The hadoop_aws/etc/ folder contains a template for building a script to set up
# all of the environment variables on which EC2 command line scripts depend.
# The account-specific variables are left blank in the template.
# The sed command executed later in this script fills in the values of these
# account-specific variables with the values this script sets up,
# $PROJECT_EC2_BASEDIR/.local${CT_PROFILE_NAME}.hadoop-ec2-env.sh
# and then we execute this script.  By collecting all of these environment
# variables within a single script, we record the complete EC2 configuration.
#
# Note: You may overrride this entire template by placing a project-specific
# copy inside $PROJECT_EC2_BASEDIR (but *NOT* inside any etc/ folder).
#
# and places the resulting script into the (hidden) script file
# Note: You need a separate template for any cluster profiles you define.
# 
if [ -f "$PROJECT_EC2_BASEDIR/hadoop-ec2-env${CT_PROFILE_NAME}.sh" ]; then
    export HADOOP_EC2_ENV_TEMPLATE="$PROJECT_EC2_BASEDIR/hadoop-ec2-env${CT_PROFILE_NAME}.sh"
elif [ -f "$HADOOP_AWS_HOME/etc/hadoop-ec2-env${CT_PROFILE_NAME}.sh" ]; then
    export HADOOP_EC2_ENV_TEMPLATE="$HADOOP_AWS_HOME/etc/hadoop-ec2-env${CT_PROFILE_NAME}.sh"
else
    echo "Error: $HADOOP_AWS_HOME/etc/hadoop-ec2-env${CT_PROFILE_NAME}.sh does not exist!"
    return
fi
#echo 'HADOOP_EC2_ENV_TEMPLATE='$HADOOP_EC2_ENV_TEMPLATE

# The hadoop_aws/etc/ folder also contains a template for a script that will be
# run on the remote instances after they are booted.
#
# Note: You may overrride this entire template as well by placing a project-specific
# copy inside $PROJECT_EC2_BASEDIR (but *NOT* inside any etc/ folder).
# 
if [ -f "$PROJECT_EC2_BASEDIR/hadoop-ec2-init-remote.sh" ]; then
    export USER_DATA_FILE_TEMPLATE="$PROJECT_EC2_BASEDIR/hadoop-ec2-init-remote.sh"
elif [ -f "$HADOOP_AWS_HOME/etc/hadoop-ec2-init-remote.sh" ]; then
    export USER_DATA_FILE_TEMPLATE="$HADOOP_AWS_HOME/etc/hadoop-ec2-init-remote.sh"
else
    echo "Error: $EC2_BASEDIR/hadoop-ec2-init-remote.sh does not exist!"
    return
fi
echo 'USER_DATA_FILE_TEMPLATE='$USER_DATA_FILE_TEMPLATE

# Collect authentication information from the contents of the EC2 keys directory
# so that it can be used by the EC2 API Tools to communicate securely with the
# AWS servers.
#
# TODO CSc The $EC2_PRIVATE_KEY and $EC2_CERT files seem like they should be
# AWS Account-specific, but they're apparently not. We need to figure out why.
# These variables do need to get set up, even though I don't see any Bash scripts
# that actually use them (besides bixo/ec2/hadoop-aws/etc/hadoop-ec2-env.sh,
# which I modified to use $AWS_KEYDIR instead). The AWS documentation
# says they're needed to support SOAP requests, so the EC2 support tools
# (e.g., ec2-describe-group) must be making SOAP requests.
#
# TODO CSc Go through scripts documenting what each of these authentication
# objects is, the term(s) used in the Amazon EC2 documentation to refer to them,
# links into the Amazon EC2 documentation, etc.
#
export EC2_PRIVATE_KEY=`find $AWS_KEYDIR  -path '*/pk*.pem'`
export EC2_CERT=`find $AWS_KEYDIR -path '*/cert*.pem'`
if [ -z "$KEY_NAME" ]; then
    export KEY_NAME=`find $AWS_KEYDIR -maxdepth 1 -path '*/id_rsa-*' | sed -e "s|^.*id_rsa-\(.*\)$|\1|"`
fi
echo 'Using AWS Key-Pair: '$KEY_NAME

# Fill in the values of the account-specific shell variables using the contents of
# files expected to live inside the EC2 keys directory.
#
export AWS_ACCOUNT_ID_FILE=$AWS_KEYDIR/accountid
export AWS_ACCESS_KEY_FILE=$AWS_KEYDIR/accesskey
export AWS_SECRET_KEY_FILE=$AWS_KEYDIR/secretkey

export AWS_ACCOUNT_ID=`cat $AWS_ACCOUNT_ID_FILE`
export AWS_ACCESS_KEY_ID=`cat $AWS_ACCESS_KEY_FILE`
export AWS_SECRET_ACCESS_KEY=`cat $AWS_SECRET_KEY_FILE`

# Map the template for setting up all the AWS shell variables into a complete script,
# using the values of these account-specific variables to fill in the blanks.
# Save it in a hidden file inside this directory (where all of these variables can
# be easily examined by a human).
#
export HADOOP_EC2_ENV=$PROJECT_EC2_BASEDIR/.local${CT_PROFILE_NAME}.hadoop-ec2-env.sh
sed -e "s|^AWS_ACCOUNT_ID=|AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID}|" \
    -e "s|^AWS_ACCESS_KEY_ID=|AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}|" \
    -e "s|^AWS_SECRET_ACCESS_KEY=|AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}|" \
    -e "s|^KEY_NAME=|KEY_NAME=${KEY_NAME}|" \
    -e "s|^EXTRA_INIT_CMDS=|EXTRA_INIT_CMDS=\"${EXTRA_INIT_CMDS}\"|" \
      $HADOOP_EC2_ENV_TEMPLATE > $HADOOP_EC2_ENV
echo 'HADOOP_EC2_ENV='$HADOOP_EC2_ENV

# Execute the resulting file, thereby defining all of the AWS shell variables.
#
. $HADOOP_EC2_ENV

# Remind the user that we've set up some options providing security parameters
# for doing SSH into EC2.
#
echo ""
echo 'You may use "ssh $SSH_OPTS ..." for standard ssh commands against EC2.'
echo ""
