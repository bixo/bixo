#!/bin/sh
# -------------------------------------------------------------------
# Environmental variables:
#
# JETS3T_HOME  Points to the home directory of a JetS3t distribution.
#
# JAVA_HOME    The home directory of the Java Runtime Environment or 
#              Java Development Kit to use. 
# -------------------------------------------------------------------

: ${JETS3T_HOME:-""}
: ${JAVA_HOME:-""}

# Check the JETS3T_HOME directory

if [ -z "$JETS3T_HOME" ]; then
  # Try to find the home directory, assuming this script is in $JETS3T_HOME/bin
  SCPT_DIR=`dirname $0`
  if [ "$SCPT_DIR" = "." ]; then
    JETS3T_HOME=..
  else    
    JETS3T_HOME=`dirname $SCPT_DIR`
  fi
fi

if [ ! -d $JETS3T_HOME/jars ]; then
  echo "Please set the environment variable JETS3T_HOME"
  exit 1
fi

# Check the JAVA_HOME directory
if [ -z "$JAVA_HOME" ]; then
  # Test whether the 'java' program is available in the system path.
  java -version 2> /dev/null
  if [ $? -gt 0 ]; then
    echo "Please set the environment variable JAVA_HOME"
    exit 1
  else
    EXEC=java
  fi
else
  EXEC=$JAVA_HOME/bin/java
fi

# echo "JetS3t path: $JETS3T_HOME"
# echo "Java path: $EXEC"

# -------------------------------------------------------------------

# Include configurations directory in classpath
CP=$JETS3T_HOME/configs

# Include resources directory in classpath
CP=$CP:$JETS3T_HOME/resources

# Include libraries in classpath
CP=$CP:$JETS3T_HOME/jars/jets3t-0.7.0.jar
CP=$CP:$JETS3T_HOME/jars/jets3t-gui-0.7.0.jar
CP=$CP:$JETS3T_HOME/jars/uploader-0.7.0.jar
CP=$CP:$JETS3T_HOME/libs/commons-logging/commons-logging-1.1.1.jar
CP=$CP:$JETS3T_HOME/libs/commons-codec/commons-codec-1.3.jar
CP=$CP:$JETS3T_HOME/libs/commons-httpclient/commons-httpclient-3.1.jar
CP=$CP:$JETS3T_HOME/libs/misc/BareBonesBrowserLaunch.jar
CP=$CP:$JETS3T_HOME/libs/logging-log4j/log4j-1.2.15.jar

OSX_APP_NAME=-Xdock:name="JetS3t Uploader"

# OutOfMemory errors? Increase the memory available by changing -Xmx128M

"$EXEC" -Xmx128M -classpath "$CP" "$OSX_APP_NAME" org.jets3t.apps.uploader.Uploader
