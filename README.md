A Simple Hadoop Sort Example
===========================

This is source to run a simple Hadoop sort.

You need Ivy and Ant installed properly.  I had to symlink ivy.jar:

    $ cd /usr/share/ant/lib
    $ ln -s /usr/share/java/ivy.jar

The you can run this demo like so:

    $ ant

Ivy downloads all the dependencies; Hadoop runs locally in single-node mode and makes a Sequence file in /tmp full of random integers; Sorts; and output the Sorted integers.

You can also run these classes inside of Eclipse with the Eclipse-Hadoop plugin.  I got it [here (hadoop-eclipse-plugin-0.20.3-SNAPSHOT.jar)](https://issues.apache.org/jira/secure/attachment/12460491/hadoop-eclipse-plugin-0.20.3-SNAPSHOT.jar).  To install I copied the jar into $ECLIPSE_HOME/dropins.

-KB


