The Actord-D (actord) project provides a server, implemented in Scala,
that provides memcached-wire-protocol compatibility.  It's a platform
to implement next-generation, scale-out, horizontal storage concepts.

To compile and install, use the maven tool:

  mvn install

To start actord:

  java -cp [big-long-classpath] ff.actord.Main

To see some command-line help or usage info:

  java -cp [big-long-classpath] ff.actord.Main -h

Or, start it using the maven tool (see the pom.xml config file):

  mvn antrun:run

And, more maven targets:

  mvn clean
  mvn compile
  mvn test
