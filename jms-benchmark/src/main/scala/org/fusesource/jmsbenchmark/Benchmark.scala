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

import scala.collection.mutable.{ListBuffer, HashMap}

import java.io.{FileInputStream, PrintStream, FileOutputStream, File}
import org.apache.felix.gogo.commands.basic.DefaultActionPreparator
import scala.collection.JavaConversions
import java.lang.{String, Class}
import org.apache.felix.gogo.commands.{Option => option, Argument => argument, Command => command, CommandException, Action}
import org.apache.felix.service.command.CommandSession
import java.util.concurrent.{TimeUnit, TimeoutException}
import org.ocpsoft.prettytime.PrettyTime
import java.util.Date
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.annotation.{JsonProperty, JsonInclude}
import scala.reflect.BeanProperty
import scala._
import scala.Some

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
      import scala.collection.JavaConversions._
      if( p.prepare(action, session, args.toList) ) {
        action.execute(session)
      }
    } catch {
      case x:CommandException=>
        println(x.getMessage)
        System.exit(-1);
    }
  }

  val MAPPER = new ObjectMapper()
  MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL)

  def parse_scenario_name(name: String) = {
    MAPPER.readValue("{" + name + "}", classOf[java.util.TreeMap[String, Object]])
  }
}

class ScenarioReport {
  @BeanProperty
  var parameters = new java.util.TreeMap[String, Object]();
  @BeanProperty
  var timestamp = Array[Long]()
  @BeanProperty
  @JsonProperty("producer tp")
  var producer_tp = Array[Long]()
  @BeanProperty
  @JsonProperty("consumer tp")
  var consumer_tp = Array[Long]()
  @BeanProperty
  @JsonProperty("max latency")
  var max_latency = Array[Long]()
  @BeanProperty
  var errors = Array[Long]()
}

class BenchmarkReport {
  @BeanProperty
  var benchmark_settings = new java.util.TreeMap[String, Object]()
  @BeanProperty
  var scenarios = Array[ScenarioReport]()
}


@command(scope="stomp", name = "benchmark", description = "The Stomp benchmarking tool")
class Benchmark extends Action {
  import Benchmark._

  @option(name = "--continue", description = "Should we continue from the last run?  Avoid re-running previously run scenarios.")
  var continue = true

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

  @option(name = "--skip", description = "Comma separated list of tests to skip.")
  var skip = ""

  @option(name = "--max-clients", description = "Max number of clients to run in parallel")
  var max_clients = 200

  @option(name = "--max-destinations", description = "Max number of destinations to use")
  var max_destinations = 100

  @option(name = "--show-skips", description = "Should skipped scenarios be displayed.")
  var show_skips = false

  def json_format[T](value:List[T]):String = {
    "[ "+value.mkString(", ")+" ]"
  }

  var benchmark_settings = new java.util.TreeMap[String, Object]()
  var scenario_reports = new java.util.LinkedHashMap[java.util.TreeMap[String, Object], ScenarioReport]()

  def have_scenario_report(name:String) = {
    val parameters = parse_scenario_name(name)
    scenario_reports.containsKey(parameters)
  }

  def add_scenario_report(name:String, samples:List[DataSample]) = {
    import scala.collection.JavaConversions._

    val scenario = new ScenarioReport
    scenario.parameters = parse_scenario_name(name)
    scenario.timestamp = samples.map(_.time).toArray
    scenario.consumer_tp = samples.map(_.consumed).toArray
    scenario.producer_tp = samples.map(_.produced).toArray
    scenario.max_latency = samples.map(_.max_latency).toArray
    scenario.errors = samples.map(_.errors).toArray

    scenario_reports.put(scenario.parameters, scenario);
    val report = new BenchmarkReport
    report.benchmark_settings = benchmark_settings
    report.scenarios = scenario_reports.values().toList.toArray
    MAPPER.writerWithDefaultPrettyPrinter.writeValue(out, report)
  }

  def execute(session: CommandSession): AnyRef = {
    if( broker_name == null ) {
      broker_name = out.getName.stripSuffix(".json")
    }

    if( out.getParentFile!=null ) {
      out.getParentFile.mkdirs
    }

    // Load up the previous results so that we can just update the result file.
    if( out.exists() ) {
      val report = MAPPER.readValue(out, classOf[BenchmarkReport])
      benchmark_settings = report.benchmark_settings
      for( scenario <- report.scenarios ) {
        scenario_reports.put(scenario.parameters, scenario)
      }
    }

    if( !benchmark_settings.containsKey(broker_name) ) {
      benchmark_settings.put("broker_name", broker_name)
    }
    benchmark_settings.put("url", url)
    benchmark_settings.put("warm_up_count", warm_up_count:java.lang.Integer)

    println("===================================================================")
    println("Benchmarking %s at: %s".format(broker_name, url))
    println("===================================================================")
    run_benchmarks
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
          var i = 0
          senario.with_load_and_connect_timeout {
            with_load(s.drop(1)) {
              proc
            }
          } {
            i += 1
            if( i <= 10 ) {
              print("c")
              1000L
            } else {
              0L
            }
          }
        case None =>
          proc
      }
    }

    Thread.currentThread.setPriority(Thread.MAX_PRIORITY)
    try {
      val sample_set = with_load(scenarios) {
        for (i <- 0 until warm_up_count) {
          print("w")
          Thread.sleep(sample_interval)
        }
        scenarios.foreach(_.collection_start)
        if (is_done != null) {
          while (!is_done(scenarios)) {
            print(".")
            Thread.sleep(sample_interval)
            scenarios.foreach(_.collection_sample)
          }

        } else {
          var remaining = sc
          while (remaining > 0) {
            print(".")
            Thread.sleep(sample_interval)
            scenarios.foreach(_.collection_sample)
            remaining -= 1
          }
        }

        println("")
        scenarios.foreach {
          scenario =>
            val collected = scenario.collection_end
            if (collected.find(_.produced != 0).isDefined) {
              println("producer throughput samples : %s".format(json_format(collected.map(_.produced))))
            }
            if (collected.find(_.consumed != 0).isDefined) {
              println("consumer throughput samples : %s".format(json_format(collected.map(_.consumed))))
              println("consumer max latency samples: %s".format(json_format(collected.map(x => "%.3f ms".format(x.max_latency / 1000000.0)))))
            }
            if (collected.find(_.errors != 0).isDefined) {
              println("errors                      : %s".format(json_format(collected.map(_.errors))))
            }
            add_scenario_report(scenario.name, collected)
        }
      }
    } catch {
      case e:TimeoutException => println(e.getMessage)
    }
    Thread.currentThread.setPriority(Thread.NORM_PRIORITY)

    if( drain) {
      scenarios.headOption.foreach( _.drain )
    }
  }


  def run_benchmarks:Unit = {

    val scenarios_to_skip = Set(skip.split(",").map(_.trim):_* )
    case class ScenarioDescription(name:String, execute:()=>Unit, duration:Int=(((sample_count+warm_up_count+2)*sample_interval)+2000))
    val descriptions = ListBuffer[ScenarioDescription]()

    var client_counts = List(1)
    while( (client_counts.head*10) < max_clients ) {
      client_counts ::= (client_counts.head * 10)
    }
    client_counts = client_counts.reverse

    var destinations_counts = List(1)
    while( (destinations_counts.head*10) < max_destinations ) {
      destinations_counts ::= (destinations_counts.head * 10)
    }
    destinations_counts = destinations_counts.reverse

    for(
      persistent <- Array(false, true) ;
      mode <- Array("queue", "topic") ;
      producers <- client_counts;
      destination_count <- destinations_counts;
      consumers <- client_counts;
      message_size <- Array(10, 100, 1000, 100000, 10000000) ;
      tx_size <- Array(0, 1, 10, 100)
    ) {

      val name = """ "group": "throughput", "mode": "%s", "persistent": %s, "message_size": %s, "tx_size": %s, "destination_count": %s, "consumers": %s, "producers": %s""".format(mode, persistent, message_size, tx_size, destination_count, consumers, producers)
      var scaling_dimensions = 0
      if( message_size>10 ) scaling_dimensions += 1
      if( tx_size>0 ) scaling_dimensions += 1

      if( producers == consumers && destination_count==1 )  {
      } else if( producers == consumers && producers==destination_count )  {
      } else {
        if( producers>1 ) scaling_dimensions += 1
        if( destination_count>1 ) scaling_dimensions += 1
        if( consumers>1 ) scaling_dimensions += 1
      }

      var skip:String = null
      if ( have_scenario_report(name) ) {
        skip = "Already have the results"
      }
      else if ( scenarios_to_skip.contains("throughput") ) {
        skip = "--skip command line option"
      }
      // Skip on odds scenario combinations like more destinations than clients.
      else if ( (consumers<destination_count) || (producers<destination_count) ) {
        skip = "more destinations than clients"
      }
      else if ( consumers+producers > max_clients ) {
        skip = "--max-clients exceeded"
      }
      // When using lots of clients, only test against small txs and small messages.
      else if ( scaling_dimensions > 1 ) {
        skip = "Scales in more than one dimension."
      }

      if ( skip!=null ) {
        if ( show_skips ) {
          println()
          println("skipping  : "+name)
          println("   reason : "+skip)
        }
      } else {
        descriptions += ScenarioDescription(name, ()=>{
          benchmark(name) { g=>
            g.destination_type = mode
            g.persistent = persistent
            g.durable == persistent && mode == "topic"
            g.ack_mode = if ( persistent ) "client" else "dups_ok"
            g.message_size = message_size
            g.tx_size = tx_size
            g.destination_count = destination_count
            g.consumers = consumers
            g.producers = producers
          }
        })
      }
    }

    for(
      mode <- Array("topic", "queue") ;
      persistent <- Array(true, false)
    ) {

      val name = """ "group": "slow_consumer", "mode": "%s", "persistent": %s """.format(mode, persistent)

      var skip:String = null
      if ( have_scenario_report(name) ) {
        skip = "Already have the results"
      }
      else if ( scenarios_to_skip.contains("slow_consumer") ) {
        skip = "--skip command line option"
      }

      if ( skip!=null ) {
        if ( show_skips ) {
          println()
          println("skipping  : "+name)
          println("   reason : "+skip)
        }
      } else {
        descriptions += ScenarioDescription(name, ()=>{
          benchmark(name) { g=>
            g.destination_type = mode
            g.persistent = persistent
            g.durable == persistent && mode == "topic"
            g.ack_mode = if ( persistent ) "client" else "auto"
            g.message_size = 10
            g.producers = 1
            g.consumers = 10
            g._consumer_sleep = new SleepFn{
              override def apply(client:Scenario#Client) = {
                // the client /w id 2 will be the slow one.
                if ( client.id == 2 )  {
                  Thread.sleep(500)
                }
              }
            }
          }
        })
      }
    }


    // Latency scenarios.
    for(
      producer_rate <- Array(1000, 1000*10, 1000*100, 1000*1000)
    ) {

      val name = """ "group": "latency", "producer_rate": """.format(producer_rate)

      var skip:String = null
      if ( have_scenario_report(name) ) {
        skip = "Already have the results"
      }
      else if ( scenarios_to_skip.contains("latency") ) {
        skip = "--skip command line option"
      }

      if ( skip!=null ) {
        if ( show_skips ) {
          println()
          println("skipping  : "+name)
          println("   reason : "+skip)
        }
      } else {
        descriptions += ScenarioDescription(name, ()=>{
          benchmark(name) { g=>
            g.destination_type = "queue"
            g.persistent = false
            g.durable == false
            g.ack_mode = "dups_ok"
            g.message_size = 10
            g.producers = 1
            g.consumers = 1
            g._producer_sleep = new SleepFn {
              var initTS=0L
              var sent = 0L

              override def apply(client:Scenario#Client) = {
                sent+=1
                val now = System.nanoTime()
                val elapsed = now-initTS

                val should_have_sent = producer_rate * elapsed / TimeUnit.SECONDS.toNanos(1)
                if( should_have_sent <= sent ) {
                  // Producer has sent it's fair share.. sleep till we need to send again.
                  if( producer_rate >= 1000 ) {
                    Thread.sleep(1)
                  } else {
                    Thread.sleep( ((1000L / producer_rate)-1).min(1) )
                  }
                }
              }
              override def init(time: Long) {
                initTS=System.nanoTime()
              }
            }
          }
        })
      }
    }

    // Load up a queue for 30 seconds..
    val load_unload_samples = 60
    for(
      persistent <- Array(true, false)
    ) {

      val name = """ "group": "queue_staging", "persistent": %s """.format(persistent)
      if ( have_scenario_report(name) ) {
        skip = "Already have the results"
      }
      else if ( scenarios_to_skip.contains("queue_staging") ) {
        skip = "--skip command line option"
      }

      if ( skip!=null ) {
        if ( show_skips ) {
          println()
          println("skipping  : "+name)
          println("   reason : "+skip)
        }
      } else {
        descriptions += ScenarioDescription(name, ()=>{
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
              override def init(time: Long) { start = time }
              override def apply(client:Scenario#Client) = {
                val elapsed = System.currentTimeMillis() - start
                val midpoint = (warm_up_count+(load_unload_samples/2))*sample_interval;
                if (elapsed > midpoint ) {
                  client.shutdown()
                }
              }
            }

            // consumer will sleep until midway through the scenario.
            g._consumer_sleep = new SleepFn {
              var start = 0L
              override def init(time: Long) { start = time }
              override def apply(client:Scenario#Client) = {
                val elapsed = System.currentTimeMillis() - start
                val midpoint = (warm_up_count+(load_unload_samples/2))*sample_interval;
                if (elapsed <  midpoint ) {
                  Thread.sleep(midpoint - elapsed)
                }
              }
            }
          }
        }, ((load_unload_samples+warm_up_count)*sample_interval)+2000 )
      }
    }

    var remaining = descriptions.size
    var timeRemaining = descriptions.foldLeft(0){case (x,y)=> (x+y.duration)}
    val pretty_time = new PrettyTime();
    for ( description <- descriptions ) {
      println()
      println("%d scenarios remaining. Estimating completion at: %s.".format(remaining, pretty_time.format(new Date(System.currentTimeMillis()+(timeRemaining)))))
      description.execute()
      remaining -= 1
      timeRemaining -= description.duration
    }



  }
}
