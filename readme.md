# JMS Benchmark

A benchmarking tool for [JMS](http://en.wikipedia.org/wiki/Java_Message_Service) servers.

## Build Prep

* Install [sbt](http://code.google.com/p/simple-build-tool/wiki/Setup) but instead 
  of setting up the sbt script to use `sbt-launch.jar "$@"` please use `sbt-launch.jar "$*"` instead.
  
* run: `sbt update` in the jms-benchmark project directory

## Running the Benchmark

The benchmark is hardwired to test an ActiveMQ JMS server running locally.  

Run `sbt run --help` to get a listing
of all the command line arguments that the benchmark supports.

For each broker you are benchmarking you will typically execute:

    sbt run reports/foo-3.2.json

The benchmarking tool will then execute a large number of predefined 
usage scenarios and gather the performance metrics for each.  Those metrics
will get stored in a `reports/foo-3.2.json` file.  

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

    sbt run --provider stomp --url tcp://localhost:61613 --user-name admin --password password reports/boxname/apollo-1.0-beta2.json

### Running against ActiveMQ 5.6-SNAPSHOT

[Apache ActiveMQ](http://activemq.apache.org) is the most popular open source JMS provider.

1. Once installed, start the server by running:

    cd ${ACTIVEMQ_HOME}
    export ACTIVEMQ_OPTS_MEMORY="-Xmx2g -XX:+UseLargePages"
    ./bin/activemq console xbean:conf/activemq-specjms.xml

2. Run the benchmark:

    sbt run --provider activemq --url tcp://localhost:61616 reports/boxname/activemq-5.6-SNAPSHOT.json


