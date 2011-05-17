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
it and updating to the line which defines the `broker_files` variable (around line 32).

    var broker_files = ['foo-3.2.json', 'cheese-1.0.json']


