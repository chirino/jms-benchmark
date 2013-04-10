/**
 * Copyright (C) 2009-2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.jmsbenchmark

import scala.collection.mutable.HashMap

import java.io.{PrintStream, FileOutputStream, File}
import org.apache.felix.gogo.commands.basic.DefaultActionPreparator
import collection.JavaConversions
import java.lang.{String, Class}
import org.apache.felix.gogo.commands.{Option => option, Argument => argument, Command => command, CommandException, Action}
import org.apache.felix.service.command.CommandSession

object Benchmark {
  def main(args: Array[String]):Unit = {
    val session = new CommandSession {
      def getKeyboard = System.in
      def getConsole = System.out
      def put(p1: String, p2: AnyRef) = {}
      def get(p1: String) = null
      def format(p1: AnyRef, p2: Int) = throw new UnsupportedOperationException
      def execute(p1: CharSequence) = throw new UnsupportedOperationException
      def convert(p1: Class[_], p2: AnyRef) = throw new UnsupportedOperationException
      def close = {}
    }

    val action = new Benchmark()
    val p = new DefaultActionPreparator
    try {
      import collection.JavaConversions._
      if( p.prepare(action, session, args.toList) ) {
        action.execute(session)
      }
    } catch {
      case x:CommandException=>
        println(x.getMessage)
        System.exit(-1);
    }
  }
}

@command(scope="stomp", name = "benchmark", description = "The Stomp benchmarking tool")
class Benchmark extends Action {

  @option(name = "--provider", description = "The type of provider being benchmarked.")
  var provider:String = "activemq"

  @option(name = "--broker-name", description = "The name of the broker being benchmarked.")
  var broker_name:String = _

  @option(name = "--url", description = "server url")
  var url = "tcp://127.0.0.1:61616"

  @option(name = "--user-name", description = "login name to connect with")
  var user_name:String = null
  @option(name = "--password", description = "password to connect with")
  var password:String = null

  @option(name = "--sample-count", description = "number of samples to take")
  var sample_count = 15
  @option(name = "--sample-interval", description = "number of milli seconds that data is collected.")
  var sample_interval = 1000
  @option(name = "--warm-up-count", description = "number of warm up samples to ignore")
  var warm_up_count = 3

  @argument(index=0, name = "out", description = "The file to store benchmark metrics in", required=true)
  var out:File = _

  @option(name = "--queue-prefix", description = "prefix used for queue names.")
  var queue_prefix = ""
  @option(name = "--topic-prefix", description = "prefix used for topic names.")
  var topic_prefix = ""

  @option(name = "--drain-timeout", description = "How long to wait for a drain to timeout in ms.")
  var drain_timeout = 3000L

  @option(name = "--display-errors", description = "Should errors get dumped to the screen when they occur?")
  var display_errors = false

  @option(name = "--allow_worker_interrupt", description = "Should worker threads get interrupted if they fail to shutdown quickly?")
  var allow_worker_interrupt = false

  @option(name = "--skip", description = "Comma seperated list of tests to skip.")
  var skip = ""

  var samples = HashMap[String, List[DataSample]]()

  def json_format[T](value:List[T]):String = {
    "[ "+value.mkString(", ")+" ]"
  }

  def execute(session: CommandSession): AnyRef = {
    if( broker_name == null ) {
      broker_name = out.getName.stripSuffix(".json")
    }

    println("===================================================================")
    println("Benchmarking %s at: %s".format(broker_name, url))
    println("===================================================================")

    run_benchmarks
    if( out.getParentFile!=null ) {
      out.getParentFile.mkdirs
    }
    val os = new PrintStream(new FileOutputStream(out))
    os.println("{")
    os.println("""  "benchmark_settings": {""")
    os.println("""    "broker_name": "%s",""".format(broker_name))
    os.println("""    "url": "%s",""".format(url))
    os.println("""    "sample_count": %d,""".format(sample_count))
    os.println("""    "sample_interval": %d,""".format(sample_interval))
    os.println("""    "warm_up_count": %d""".format(warm_up_count))
    os.println("""  },""")
    os.print("""  "scenarios":[""")

    os.print(samples.map{ case (name, sample) =>
      """  {
    "parameters": { %s },
    "timestamp": %s,
    "producer tp": %s,
    "consumer tp": %s,
    "max latency": %s,
    "errors": %s
  }""".format(
        name,
        json_format(sample.map(_.time)),
        json_format(sample.map(_.produced)),
        json_format(sample.map(_.consumed)),
        json_format(sample.map(_.max_latency)),
        json_format(sample.map(_.errors))
      )
    }.mkString(",\n"))
    os.println("]\n}")

    os.close
    println("===================================================================")
    println("Stored: "+out)
    println("===================================================================")
    null
  }

  private def benchmark(name:String, drain:Boolean=true, sc:Int=sample_count, is_done: (List[Scenario])=>Boolean = null)(init_func: (Scenario)=>Unit ):Unit = {
    multi_benchmark(List(name), drain, sc, is_done) { scenarios =>
      init_func(scenarios.head)
    }
  }

  def create_scenario:JMSClientScenario = {
    val clazz = provider.toLowerCase match {
      case "activemq" => "org.fusesource.jmsbenchmark.ActiveMQScenario"
      case "stomp" => "org.fusesource.jmsbenchmark.StompScenario"
      case _ => provider
    }
    getClass.getClassLoader.loadClass(clazz).newInstance().asInstanceOf[JMSClientScenario]
  }

  private def multi_benchmark(names:List[String], drain:Boolean=true, sc:Int=sample_count, is_done: (List[Scenario])=>Boolean = null)(init_func: (List[Scenario])=>Unit ):Unit = {
    Runtime.getRuntime.gc()
    val scenarios:List[Scenario] = names.map { name=>
      val scenario = create_scenario
      scenario.name = name
      scenario.sample_interval = sample_interval
      scenario.url = url
      scenario.user_name = user_name
      scenario.password = password
      scenario.queue_prefix = queue_prefix
      scenario.topic_prefix = topic_prefix
      scenario.drain_timeout = drain_timeout
      scenario.display_errors = display_errors
      scenario.allow_worker_interrupt = allow_worker_interrupt
      scenario
    }

    init_func(scenarios)

    scenarios.foreach{ scenario=>
      scenario.destination_name = if( scenario.destination_type == "queue" ) {
       "loadq"
      } else {
       "loadt"
      }
    }

    println()
    println("starting scenario : %s ".format(names.mkString(" and ")))
      print("sampling : ")

    def with_load[T](s:List[Scenario])(proc: => T):T = {
      s.headOption match {
        case Some(senario) =>
          senario.with_load {
            with_load(s.drop(1)) {
              proc
            }
          }
        case None =>
          proc
      }
    }

    Thread.currentThread.setPriority(Thread.MAX_PRIORITY)
    val sample_set = with_load(scenarios) {
      for( i <- 0 until warm_up_count ) {
        Thread.sleep(sample_interval)
        print(".")
      }
      scenarios.foreach(_.collection_start)

      if( is_done!=null ) {
        while( !is_done(scenarios) ) {
          print(".")
          Thread.sleep(sample_interval)
          scenarios.foreach(_.collection_sample)
        }

      } else {
        var remaining = sc
        while( remaining > 0 ) {
          print(".")
          Thread.sleep(sample_interval)
          scenarios.foreach(_.collection_sample)
          remaining-=1
        }
      }


      println(".")
      scenarios.foreach{ scenario=>
        val collected = scenario.collection_end
        if( collected.find( _.produced != 0 ).isDefined ) {
          println("producer throughput samples : %s".format(json_format(collected.map(_.produced) )) )
        }
        if( collected.find( _.consumed != 0 ).isDefined ) {
          println("consumer throughput samples : %s".format(json_format(collected.map(_.consumed) )) )
          println("consumer max latency samples: %s".format(json_format(collected.map(x => "%.3f ms".format(x.max_latency / 1000000.0)) )) )
        }
        if( collected.find( _.errors != 0 ).isDefined ) {
          println("errors                      : %s".format(json_format(collected.map(_.errors) )) )
        }
        samples.put(scenario.name, collected)
      }
    }
    Thread.currentThread.setPriority(Thread.NORM_PRIORITY)

    if( drain) {
      scenarios.headOption.foreach( _.drain )
    }
  }


  def run_benchmarks:Unit = {

    val scenarios_to_skip = Set(skip.split(",").map(_.trim):_* )

    // Load up a queue for 30 seconds..
    val load_unload_samples = 60
    for(
      persistent <- Array(true, false)
    ) {

      var skip:String = null
      if ( scenarios_to_skip.contains("queue_staging") ) {
        skip = "--skip command line option"
      }

      val name = """ "group": "queue_staging", "persistent": %s """.format(persistent)
      if ( skip!=null ) {
        println()
        println("skipping  : "+name)
        println("   reason : "+skip)
      } else {
        benchmark(name, sc=load_unload_samples) { g=>
          g.destination_type = "queue"
          g.persistent = persistent
          g.ack_mode = "auto"
          g.message_size = 10
          g.tx_size = 0
          g.producers = 10
          g.consumers = 10

          // producer will sleep midway..
          g._producer_sleep = new SleepFn {
            var start = 0L
            def init(time: Long) { start = time }
            def apply(client:Scenario#Client) = {
              val elapsed = System.currentTimeMillis() - start
              val midpoint = (warm_up_count+(load_unload_samples/2))*sample_interval;
              if (elapsed > midpoint ) {
                client.shutdown()
                0
              } else {
                0
              }
            }
          }

          // consumer will sleep until midway through the scenario.
          g._consumer_sleep = new SleepFn {
            var start = 0L
            def init(time: Long) { start = time }
            def apply(client:Scenario#Client) = {
              val elapsed = System.currentTimeMillis() - start
              val midpoint = (warm_up_count+(load_unload_samples/2))*sample_interval;
              if (elapsed <  midpoint ) {
                midpoint - elapsed
              } else {
                0
              }
            }
          }
        }
      }
    }

    for(
      mode <- Array("queue", "topic") ;
      persistent <- Array(true, false) ;
      selector_complexity <- Array(0) ; // <- Array(0,1,2,3) ; // not yet implemented.
      consumers <- Array(1000, 100, 10, 1) ; // Array(1, 10, 100, 1000, 10000) ;
      producers <- Array(1000, 100, 10, 1) ; // Array(1, 10, 100, 1000, 10000)
      message_size <- Array(10000000, 100000, 1000, 100, 10) ;
      tx_size <- Array(100, 10, 1, 0) ;
      destination_count <- Array(1, 10, 100, 1000)  // Array(1, 10, 100, 1000, 10000) ;
    ) {

      var skip:String = null
      if ( scenarios_to_skip.contains("throughput") ) {
        skip = "--skip command line option"
      }
      // Skip on odds scenario combinations like more destinations than clients.
      else if ( (consumers<destination_count) || (producers<destination_count) ) {
        skip = "more destinations than clients"
      }
      // When using lots of clients, only test against small txs and small messages.
      else if ( (producers>100 || consumers>100) && (tx_size > 1 || message_size>10) ) {
        skip = "When using lots of clients, only test against small txs and small messages."
      }
      // Don't benchmark large messages /w lots of clients to avoid OOM
      else if ( message_size >= 100000 && (consumers > 1 || producers > 1 || tx_size > 1) ) {
        skip = "Don't benchmark large messages /w lots of clients."
      }
      // Don't benchmark large transactions /w lots of clients to avoid OOM
      else if ( tx_size >= 100 && (consumers > 10 || producers > 10 || tx_size > 10) ) {
        skip = "Don't benchmark large transactions /w lots of clients"
      }

      val name = """ "group": "throughput", "mode": "%s", "persistent": %s, "message_size": %s, "tx_size": %s, "selector_complexity": %s, "destination_count": %s, "consumers": %s, "producers": %s""".format(mode, persistent, message_size, tx_size, selector_complexity, destination_count, consumers, producers)
      if ( skip!=null ) {
        println()
        println("skipping  : "+name)
        println("   reason : "+skip)
      } else {

        benchmark(name) { g=>
          g.destination_type = mode
          g.persistent = persistent
          g.durable == persistent && mode == "topic"
          g.ack_mode = if ( persistent ) "client" else "auto"
          g.message_size = message_size
          g.tx_size = tx_size
          g.destination_count = destination_count
          g.consumers = consumers
          g.producers = producers
        }
      }
    }

    for(
      mode <- Array("topic", "queue") ;
      persistent <- Array(true, false)
    ) {
      var skip:String = null
      if ( scenarios_to_skip.contains("slow_consumer") ) {
        skip = "--skip command line option"
      }

      val name = """ "group": "slow_consumer", "mode": "%s", "persistent": %s """.format(mode, persistent)
      if ( skip!=null ) {
        println()
        println("skipping  : "+name)
        println("   reason : "+skip)
      } else {
        benchmark(name) { g=>
          g.destination_type = mode
          g.persistent = persistent
          g.durable == persistent && mode == "topic"
          g.ack_mode = if ( persistent ) "client" else "auto"
          g.message_size = 10
          g.producers = 1
          g.consumers = 10
          g._consumer_sleep = new SleepFn{
            def apply(client:Scenario#Client) = {
              // the client /w id 2 will be the slow one.
              if ( client.id == 2 )  {
                500
              } else {
                0
              }
            }
            def init(time: Long) {}
          }
        }
      }
    }
  }
}
