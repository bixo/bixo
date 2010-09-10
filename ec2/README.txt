The scripts in this directory can be used to create a Bixo-enabled cluster in EC2

See also http://bixo.101tec.com/documentation/running-bixo-in-ec2/ for more in-depth
information about how to set up for running Bixo in EC2.

==================================================================
Configuring
==================================================================

  To use these scripts:

  > cd <Bixo project root>/bin/ec2
  > . setenv.sh

  This sets all the necessary environment variables from <project root>/bin/ec2/etc/hadoop-ec2-env.sh
  
  The file ".local.awskey-path" must exist in the project dir and contain a path to the keys to use.

  They key directory should have the following files:

    accountid
    accesskey
    secretkey
    cert-xxxxxxxxx.pem
    pk-xxxxxxxxxx.pem  
    id_rsa-<keypair name> - used by ssh and hadoop-ec2-env.sh

==================================================================
Starting a cluster
==================================================================

  > hadoop-ec2 launch-cluster <cluster-name> N

  Where <cluster-name> is the name you wish to give this cluster instance. You may have many concurrent cluster instances.
  And where N is the number of SLAVE nodes. Total number of nodes will be N+1.

==================================================================
terminating a cluster:
==================================================================

  > hadoop-ec2 terminate-cluster <cluster-name>

  You must type "yes" at the prompt.

==================================================================
Starting a remote job
==================================================================

  > hadoop-ec2 push <cluster-name> path/to/app.jar
  > hadoop-ec2 screen <cluster-name>
  > hadoop jar app.jar [args]

==================================================================
Notes
==================================================================

  These clusters do not create a secondary namenode, so in theory should be recycled after X jobs.
