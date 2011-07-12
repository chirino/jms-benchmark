# JMS Benchmark

A benchmarking tool for [JMS](http://en.wikipedia.org/wiki/Java_Message_Service) servers.

## Build Prep

* Install maven
  
* run: mvn install

## Updating the Report

The `reports/report.html` file can load and display the results of multiple benchmark runs.
You can updated which benchmark results are displayed by the report.html by editing
it and updating to the line which defines the `products` variable (around line 34).

      var products = [
        'apollo-1.0-SNAPSHOT', 
        'activemq-5.4.2'
      ];

### Running against Apollo 1.0-beta2

[Apache Apollo](http://activemq.apache.org/apollo) is a new Stomp based 
message server from good folks at the [Apache ActiveMQ](http://activemq.apache.org/) 
project.

1. Follow the [getting started guide](http://activemq.apache.org/apollo/versions/1.0-beta1/website/documentation/getting-started.html) 
to install, setup, and start the server.

2. Run the benchmark with the admin credentials.  Example:

    cd jms-benchmark-stomp
    mvn exec:java -Dbox=boxname -Dserver=apollo-1.0-beta2

### Running against ActiveMQ 5.6-SNAPSHOT

[Apache ActiveMQ](http://activemq.apache.org) is the most popular open source JMS provider.

1. Once installed, start the server by running:

    cd ${ACTIVEMQ_HOME}
    export ACTIVEMQ_OPTS_MEMORY="-Xmx2g -XX:+UseLargePages"
    ./bin/activemq console xbean:conf/activemq-specjms.xml

2. Run the benchmark:

    cd jms-benchmark-activemq
    mvn exec:java -Dbox=boxname -Dserver=activemq-5.6-SNAPSHOT
    

