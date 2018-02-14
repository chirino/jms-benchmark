# JMS Benchmark

A benchmarking tool for [JMS 1.1](http://en.wikipedia.org/wiki/Java_Message_Service) servers.
The benchmark covers a wide variety of common usage scenarios.

## Servers Currently Benchmarked

* Apache ActiveMQ (Openwire protocol)
* Apache ActiveMQ Apollo (STOMP and Openwire protocols)
* HornetQ (Core protocol)

<!-- 
* RabbitMQ
-->

# Just looking for the Results?

The numbers look different depending on the Hardware and OS they are run on:

* [Amazon Linux: EC2 High-CPU Extra Large Instance](http://hiramchirino.com/jms-benchmark/ec2-c1.xlarge/index.html)
* [Ubuntu 11.10: Quad-Core 2600k Intel CPU (3.4 GHz)](http://hiramchirino.com/jms-benchmark/ubuntu-2600k/index.html)

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
    curl https://nodeload.github.com/chirino/jms-benchmark/zip/master > jms-benchmark.zip
    jar -xvf jms-benchmark.zip 
    chmod a+x ./jms-benchmark-master/bin/*
    screen ./jms-benchmark-master/bin/benchmark-all

The results will be stored in the ~/reports directory.

## Benchmarking remote Activemq from an EC2 Amazon Linux 64 bit AMI

    sudo yum install -y screen
    curl https://nodeload.github.com/chirino/jms-benchmark/zip/master > jms-benchmark.zip
    jar -xvf jms-benchmark.zip 
    chmod a+x ./jms-benchmark-master/bin/*
    screen env \
      SERVER_SETUP=false \
      SERVER_ADDRESS={activemq-endpoint} \
      ACTIVEMQ_TRANSPORT={activemq-transport} \
      ACTIVEMQ_PORT={activemq-port} \
      ACTIVEMQ_USERNAME={activemq-user} \
      ACTIVEMQ_PASSWORD={activemq-password} \
      ./bin/benchmark-activemq

The results will be stored in the ~/reports directory.
