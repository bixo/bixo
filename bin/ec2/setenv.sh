#!/bin/bash

echo "initializing environment"

BASEDIR=`pwd`

export HADOOP_AWS_HOME=$BASEDIR/hadoop-aws
export AD_AWS_HOME=$BASEDIR/asterdata-aws
export EC2_HOME=$BASEDIR/support/ec2-api-tools-1.3-30349
export JETS3T_HOME=$BASEDIR/support/jets3t-0.7.0

export PATH=$BASEDIR/bin:$EC2_HOME/bin:$HADOOP_AWS_HOME/bin:$AD_AWS_HOME:$JETS3T_HOME/bin:$PATH

export AWS_KEYDIR=$BASEDIR/awskeys

CT_PROFILE=${1:+.$1}
CT_BASE=$HADOOP_AWS_HOME/etc/hadoop-ec2-env${CT_PROFILE}.sh

[ ! -f $CT_BASE ] && echo "Error: $CT_BASE not found!" && return

export HADOOP_EC2_ENV=$BASEDIR/.local${CT_PROFILE}.hadoop-ec2-env.sh

# use this file to override the key path for aws tools
if [ -e ".local.awskey-path" ]; then
    export AWS_KEYDIR=`cat .local.awskey-path`
else
    echo "Error: .local.awskey-path does not exist!"
    return
fi

# used by ec2-tools
export EC2_PRIVATE_KEY=`find $AWS_KEYDIR  -path '*/pk*.pem'`
export EC2_CERT=`find $AWS_KEYDIR -path '*/cert*.pem'`
export ID_RSA_KEY_NAME=`find $AWS_KEYDIR -path '*/id_rsa*' | sed -e "s|^.*id_rsa-\(.*\)$|\1|"`

# used to create images and access s3
export AWS_ACCOUNT_ID_FILE=$AWS_KEYDIR/accountid
export AWS_ACCESS_KEY_FILE=$AWS_KEYDIR/accesskey
export AWS_SECRET_KEY_FILE=$AWS_KEYDIR/secretkey

export AWS_ACCOUNT_ID=`cat $AWS_ACCOUNT_ID_FILE`
export AWS_ACCESS_KEY_ID=`cat $AWS_ACCESS_KEY_FILE`
export AWS_SECRET_ACCESS_KEY=`cat $AWS_SECRET_KEY_FILE`

sed -e "s|^AWS_ACCOUNT_ID=|AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID}|" \
    -e "s|^AWS_ACCESS_KEY_ID=|AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}|" \
    -e "s|^AWS_SECRET_ACCESS_KEY=|AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}|" \
    -e "s|^KEY_NAME=|KEY_NAME=${ID_RSA_KEY_NAME}|" \
      $CT_BASE > $HADOOP_EC2_ENV

S3_AMI_BUCKET=`cat $HADOOP_EC2_ENV | egrep "^S3_BUCKET" | sed -e "s|^S3_BUCKET=\(.*\)$|\1|"`

. $HADOOP_EC2_ENV
export SSH_OPTS=$SSH_OPTS

echo 'using HADOOP_EC2_ENV: '$HADOOP_EC2_ENV
echo 'use "ssh $SSH_OPTS ..." for standard ssh commands against EC2'
echo 'the current ACCESS_KEY must have access to the S3 bucket named: '$S3_AMI_BUCKET
echo ""
