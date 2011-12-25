# JMS Benchmark

A benchmarking tool for [JMS 1.1](http://en.wikipedia.org/wiki/Java_Message_Service) servers.
The benchmark covers a wide variety of common usage scenarios.

## Servers Currently Benchmarked

* Apache ActiveMQ
* Apache ActiveMQ Apollo
* RabbitMQ
<!-- * HornetQ -->

## Running the Benchmark

Just run:

    ./bin/benchmark-all
    
or one of the server specific benchmark scripts like:

    ./bin/benchmark-activemq

Tested to work on:

* Ubuntu 11.10
* Amazon Linux
* OS X

The benchmark report will be stored in the `reports/$(hostname)` directory.

## Running the Benchmark on an EC2 Amazon Linux 64 bit AMI

If you want to run the benchmark on EC2, we recommend using at least the
c1.xlarge instance type.  Once you have the instance started just execute
the following commands on the instance:

    sudo yum install -y screen
    curl https://nodeload.github.com/chirino/jms-benchmark/tarball/master | tar -zxv 
    mv chirino-jms-benchmark-* jms-benchmark
    screen ./jms-benchmark/bin/benchmark-all

The results will be stored in the ~/reports directory.
