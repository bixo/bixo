Welcome to the JetS3t toolkit and application suite.

For further information, documentation, and links to discussion lists and 
other resources please visit the JetS3t web site: 
http://jets3t.s3.amazonaws.com/index.html


* Running Applications

Each application can be run using a script in the "bin" directory.
To run an application, such as Cockpit, run the appropriate script from
the bin directory.

Windows:
cd jets3t-0.7.0\bin
cockpit.bat

Unixy:
cd jets3t-0.7.0/bin
bash cockpit.sh


* Servlets

The JetS3t application suite now includes a servlet implementation of a
Gatekeeper to offer mediated third-party access to your S3 resources. The 
deployable WAR file for this servlet is located in the "servlets/gatekeeper"
directory.


* Configuration files

Some applications or library components read text configuration files.
Generally, these configuration files must be available in the classpath.

Example configuration files are located in the "configs" directory. The 
run scripts in the "bin" directory automatically include this "configs" 
directory in the classpath.

The configuration files include:

- jets3t.properties: Low-level toolkit configuration.
- synchronize.properties: Properties for the Synchronize application
- uploader.properties: Properties for the Uploader application
- cockpitlite.properties: Properties for the CockpitLite application
- mime.types: Maps file extensions to the appropriate mime/content type.
  For example, the "txt" extension maps to "text/plain".
- commons-logging.properties: Defines which logging implementation to use.
- log4j.properties: When Log4J is the chosen logging implementation, these
  settings control how much logging information is displayed, and the way 
  it is displayed.
- simplelog.properties: When SimpleLog is the chosen logging implementation, 
  these settings control the logging information that is displayed.


* JAR files

The compiled JetS3t code jar files are available in the "jars" directory,
and include the following:

jets3t-0.7.0.jar
  
  The JetS3t toolkit. The toolkit including the JetS3t service implemention 
  which underlies all the other JetS3t applications.
  http://jets3t.s3.amazonaws.com/toolkit/toolkit.html
  
jets3t-gui-0.7.0.jar

  Graphical user interface components used by JetS3t GUI applications such as
  Cockpit. These components are not required by the command-line Synchronize
  tool, nor by non-graphical programs you may build.

cockpit-0.7.0.jar

  Cockpit, a GUI application/applet for viewing and managing the contents of an 
  S3 account.
  http://jets3t.s3.amazonaws.com/applications/cockpit.html
  
synchronize-0.7.0.jar

  Synchronize, a console application for synchronizing directories on a computer 
  with an Amazon S3 account.
  http://jets3t.s3.amazonaws.com/applications/synchronize.html
  
cockpitlite-0.7.0.jar

  CockpitLite, a GUI application/applet for viewing and managing the contents of
  an S3 account, where the S3 account is not owned by the application's user 
  directly but is made available via the Gatekeeper servlet.
  http://jets3t.s3.amazonaws.com/applications/cockpitlite.html

uploader-0.7.0.jar

  A wizard-based GUI application/applet that S3 account holders (Service 
  Providers) may provide to clients to allow them to upload files to S3 without 
  requiring access to the Service Provider's S3 credentials
  http://jets3t.s3.amazonaws.com/applications/uploader.html
  

* Compatibility and Performance of Distributed Jar files

The class files in these jars are compiled for compatibility with Sun's JDK 
version 1.4 and later, and have debugging turned on to provide more information 
if errors occur. 

To use JetS3t in high-performance scenarios, the classes should be 
recompiled using the latest version of Java available to you, and with 
debugging turned off.


* Rebuilding JetS3t

The JetS3t distribution package includes an ANT build script (build.xml) that
allows you to easily rebuild the project yourself. For example, the following
command will recompile the JetS3t libary and applications:

ant rebuild-all

To repackage JetS3t applications or applets for redistribution:

ant repackage-applets
