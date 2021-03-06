====== Deployment procedure to an application server ======

I. Make sure you have correct: 
   * settings.xml. There should be the correct application server version :
     **  To build eXo JCR on Tomcat : Add the maven propery exo.projects.app.tomcat.version and set it to tomcat-7.x.x
     **  To build eXo JCR on JBoss : Add the maven propery exo.projects.app.jboss.version and set it to jboss-7.x.x
     **  To build eXo JCR on Jetty : Add the maven propery exo.projects.app.jetty.version and set it to jetty-8.x.x

   * The bundle (tomcat-7.x.x, jboss-7.x.x or jetty-8.x.x) should be put into the directory corresponding to the value of the maven property exo.projects.directory.dependencies.
   * exo directory structure
   * Maven version 3.0.3 (or higher)

II. Has been tested with such AS:
   * Tomcat 7.0.32
   * JBoss 7.1.1
   * Jetty 8.1.8

III. Deployment
   If you want to deploy Tomcat, go to folder "exo.jcr.applications.tomcat" and run "mvn clean install -Pdeploy" command.
   If you want to deploy Jetty, go to folder "exo.jcr.applications.Jetty" and run "mvn clean install -Pdeploy" command.
   If you want to deploy JBoss, go to folder "exo.jcr.applications.jboss" and run "mvn clean install -Pdeploy" command.

IV. If the command has executed successfully, go to exo-working directory, choose deployed server and run :
    In Unix environment
       * For Tomcat "bin/eXo.sh run" command.
       * For Jetty "/eXo.sh" command.
       * For JBoss "bin/standalone.sh" command.
    In Windows environment
       * For Tomcat "bin/eXo.bat run" command.
       * For Jetty "/eXo.bat" command.
       * For JBoss "bin/standalone.bat" command.

