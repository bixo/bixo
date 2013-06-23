The scripts in this directory can be used to create a Bixo-enabled 
cluster in EC2

If you are working with a distribution, you will need to get the 
ec2-api-tools. The Bixo project maintains a slightly older version
at https://github.com/bixo/bixo/tree/master/ec2/support. Or you 
can download the latest from http://aws.amazon.com/developertools/

See also http://openbixo.org/documentation/running-bixo-in-ec2/
for more in-depth information about how to set up for running Bixo
in EC2.

==================================================================
Configuring
==================================================================

  To use these scripts:

  > cd <Bixo project root>/ec2
  > . setenv.sh

  This sets all the necessary environment variables from
  <Bixo project root>/ec2/etc/hadoop-ec2-env.sh
  
  Note: It is also possible to extend the default Bixo EC2
  configuration by providing a project-specific setenv.sh
  file inside <Your project root>/ec2/. A template for doing so
  is in <Bixo project root>/ec2/setenv.sh.project-template.
  
  The file ".local.awskey-path" must exist in the directory
  <Bixo project root>/ec2/ and contain a path to the keys to use.

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

  > hadoop-ec2 launch-cluster <cluster-name> <#slaves> [<type> [<price>]]

  Where:
  <cluster-name> is the name you wish to give this cluster instance.
  You may have many concurrent cluster instances.
  <#slaves> is the number of SLAVE nodes. The total number of nodes
  in the cluster will be <#slaves>+1.
  <type> is the instance type of the machines in the cluster
  (defaults to $DEFAULT_INSTANCE_TYPE, which defaults to m1.small).
  <price> is the maximum spot price/hour/machine for Spot Instances
  (defaults to On-Demand instances).
  
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

  These clusters do not create a secondary namenode, so in theory
  should be recycled after X jobs.
