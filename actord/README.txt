The Actord-D (actord) project provides a server, implemented in Scala,
that provides memcached-wire-protocol compatibility.  It's a platform
to implement next-generation, scale-out, horizontal storage concepts.

To start actord:

  java -cp [big-long-classpath] ff.actord.Main

To compile and install, use the maven tool:

  mvn install

And, more maven targets:

  mvn clean
  mvn compile
  mvn test
