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

import java.util.concurrent.atomic._
import java.util.concurrent.TimeUnit._
import scala.collection.mutable.ListBuffer
import java.util.concurrent.{CountDownLatch, BrokenBarrierException, TimeoutException, CyclicBarrier}

case class DataSample(time:Long, produced:Long, consumed:Long, errors:Long, max_latency:Long)

abstract class SleepFn {
  def init(time:Long):Unit
  def apply(client:Scenario#Client):Long
}

object Scenario {
  val MESSAGE_ID:Array[Byte] = "message-id"
  val NEWLINE = '\n'.toByte
  val NANOS_PER_SECOND = NANOSECONDS.convert(1, SECONDS)
  
  implicit def toBytes(value: String):Array[Byte] = value.getBytes("UTF-8")

  def o[T](value:T):Option[T] = value match {
    case null => None
    case x => Some(x)
  }
}

trait Scenario {
  import Scenario._

  var url:String = "tcp://localhost:61616"
  var user_name:String = _
  var password:String = _

  var _producer_sleep: SleepFn = new SleepFn { def apply(client:Scenario#Client) = 0; def init(time: Long) {}  }
  def producer_sleep_= (new_value: Int) = _producer_sleep = new SleepFn{ def apply(client:Scenario#Client) = new_value; def init(time: Long) {}  }

  var _consumer_sleep: SleepFn = new SleepFn { def apply(client:Scenario#Client) = 0; def init(time: Long) {}  }
  def consumer_sleep_= (new_value: Int) = _consumer_sleep = new SleepFn { def apply(client:Scenario#Client) = new_value; def init(time: Long) {}  }

  var producers = 1
  var producers_per_sample = 0

  var consumers = 1
  var consumers_per_sample = 0
  var sample_interval = 1000

  var message_size = 1024
  var persistent = false

  var headers = Array[Array[(String,String)]]()
  var selector:String = null
  var no_local = false
  var durable = false
  var ack_mode = "auto"
  var messages_per_connection = -1L
  var display_errors = false
  var tx_size = 0

  var destination_type = "queue"
  private var _destination_name: () => String = () => "load"
  def destination_name = _destination_name()
  def destination_name_=(new_name: String) = _destination_name = () => new_name
  def destination_name_=(new_func: () => String) = _destination_name = new_func
  var destination_count = 1

  val producer_counter = new AtomicLong()
  val consumer_counter = new AtomicLong()
  val error_counter = new AtomicLong()
  val max_latency = new AtomicLong()
  val done = new AtomicBoolean()

  var queue_prefix = ""
  var topic_prefix = ""
  var name = "custom"

  var drain_timeout = 2000L

  def run() = {
    print(toString)
    println("--------------------------------------")
    println("     Running: Press ENTER to stop")
    println("--------------------------------------")
    println("")

    with_load {

      // start a sampling client...
      val sample_thread = new Thread() {
        override def run() = {
          
          def print_rate(name: String, periodCount:Long, totalCount:Long, nanos: Long) = {

            val rate_per_second: java.lang.Float = ((1.0f * periodCount / nanos) * NANOS_PER_SECOND)
            println("%s total: %,d, rate: %,.3f per second".format(name, totalCount, rate_per_second))
          }

          try {
            var start = System.nanoTime
            var total_producer_count = 0L
            var total_consumer_count = 0L
            var total_error_count = 0L
            collection_start
            while( !done.get ) {
              Thread.sleep(sample_interval)
              val end = System.nanoTime
              collection_sample
              val collected = collection_end

              if ( producers > 0 ) {
                val count = collected.last.produced
                total_producer_count += count
                if ( total_producer_count > 0 ) {
                  print_rate("Producer", count, total_producer_count, end - start)
                }
              }

              if ( consumers > 0 ) {
                val count = collected.last.consumed
                total_consumer_count += count
                if ( total_consumer_count > 0 ) {
                  print_rate("Consumer", count, total_consumer_count, end - start)
                }
              }

              val count = collected.last.errors
              total_error_count += count
              if ( total_error_count > 0 ) {
                if ( count != 0 ) {
                  print_rate("Error", count, total_error_count, end - start)
                }
              }

              start = end
            }
          } catch {
            case e:InterruptedException =>
          }
        }
      }
      sample_thread.start()

      System.in.read()
      done.set(true)

      sample_thread.interrupt
      sample_thread.join
    }

  }

  override def toString() = {
    "--------------------------------------\n"+
    "Scenario Settings\n"+
    "--------------------------------------\n"+
    "  destination_type      = "+destination_type+"\n"+
    "  queue_prefix          = "+queue_prefix+"\n"+
    "  topic_prefix          = "+topic_prefix+"\n"+
    "  destination_count     = "+destination_count+"\n" +
    "  destination_name      = "+destination_name+"\n" +
    "  sample_interval (ms)  = "+sample_interval+"\n" +
    "  \n"+
    "  --- Producer Properties ---\n"+
    "  producers             = "+producers+"\n"+
    "  message_size          = "+message_size+"\n"+
    "  persistent            = "+persistent+"\n"+
    "  producer_sleep (ms)   = "+_producer_sleep(null)+"\n"+
    "  headers               = "+headers.mkString(", ")+"\n"+
    "  \n"+
    "  --- Consumer Properties ---\n"+
    "  consumers             = "+consumers+"\n"+
    "  consumer_sleep (ms)   = "+_consumer_sleep(null)+"\n"+
    "  selector              = "+selector+"\n"+
    "  durable               = "+durable+"\n"+
    ""

  }

  protected def headers_for(i:Int) = {
    if ( headers.isEmpty ) {
      Array[(String, String)]()
    } else {
      headers(i%headers.size)
    }
  }

  var data_samples = ListBuffer[DataSample]()

  def collection_start: Unit = {
    producer_counter.set(0)
    consumer_counter.set(0)
    error_counter.set(0)
    max_latency.set(0)
    data_samples = ListBuffer[DataSample]()
  }

  def update_max_latency(value:Long) = {
    var was = max_latency.get
    while ( value > was && !max_latency.compareAndSet(was, value) ) {
      // someone else changed it before we could update it, lets
      // get the update and see if we still need to update.
      was = max_latency.get
    }
  }

  def collection_end = data_samples.toList

  trait Client {
    def id:Int
    def start():Unit
    def shutdown():Unit
    def wait_for_shutdown:Unit
  }

  var producer_clients = List[Client]()
  var consumer_clients = List[Client]()

  var connected_latch:CountDownLatch = null
  var apply_load_latch:CountDownLatch = null

  def with_load[T](func: =>T ):T = {
    var i = 0
    with_load_and_connect_timeout(func) {
      i += 1
      if( i <= 10 ) {
        1000L
      } else {
        0L
      }
    }
  }

  def with_load_and_connect_timeout[T](func: =>T)(continue_connecting: =>Long):T = {
    done.set(false)

    _producer_sleep.init(System.currentTimeMillis())
    _consumer_sleep.init(System.currentTimeMillis())

    connected_latch = new CountDownLatch(producers + consumers);
    apply_load_latch = new CountDownLatch(1);

    for (i <- 0 until producers) {
      val client = createProducer(i)
      producer_clients ::= client
      client.start()
    }

    for (i <- 0 until consumers) {
      val client = createConsumer(i)
      consumer_clients ::= client
      client.start()
    }

    try {
      var timeout = 0L
      var done_connect = false
      while( !done_connect ) {
        if ( connected_latch.await(timeout, MILLISECONDS) ) {
          done_connect = true
          apply_load_latch.countDown();
        }  else {
          timeout = continue_connecting
          if ( timeout <= 0 ) {
            apply_load_latch.countDown();
            done.set(true)
            throw new TimeoutException("All clients could not connect.  Failing connections: "+connected_latch.getCount);
          }
        }
      }
      func
    } finally {
      done.set(true)
      for( client <- producer_clients ) {
        client.shutdown
      }
      for( client <- producer_clients ) {
        client.wait_for_shutdown
      }
      producer_clients = List()
      // wait for the threads to finish..
      for( client <- consumer_clients ) {
        client.shutdown
      }
      for( client <- consumer_clients ) {
        client.wait_for_shutdown
      }
      consumer_clients = List()
    }
  }

  def drain = {
    done.set(false)
    if( destination_type=="queue" || destination_type=="raw_queue" || durable==true ) {
      print("draining")
      consumer_counter.set(0)
      var drained = 0L

      val original_tx_size = tx_size
      val original_consumer_sleep = _consumer_sleep
      val original_ack_mode = ack_mode

      // Lets change the consumer config to consume as fast as possible.
      tx_size = 0
      consumer_sleep_=(0)
      ack_mode = "dups_ok"

      var consumer_clients = List[Client]()
      for (i <- 0 until destination_count) {
        val client = createConsumer(i)
        consumer_clients ::= client
        client.start()
      }

      // Keep sleeping until we stop draining messages.
      try {
        Thread.sleep(drain_timeout);
        def done() = {
          val c = consumer_counter.getAndSet(0)
          drained += c
          c == 0
        }
        while( !done ) {
          print(".")
          Thread.sleep(drain_timeout);
        }
      } finally {
        done.set(true)
        for( client <- consumer_clients ) {
          client.shutdown
        }
        for( client <- consumer_clients ) {
          client.wait_for_shutdown
        }
        println(". (drained %d)".format(drained))
        tx_size = original_tx_size
        _consumer_sleep = original_consumer_sleep
        ack_mode = original_ack_mode
      }
    }
  }


  def collection_sample: Unit = {

    val now = System.currentTimeMillis()
    val data = DataSample(
      time=now,
      produced=producer_counter.getAndSet(0),
      consumed=consumer_counter.getAndSet(0),
      errors=error_counter.getAndSet(0),
      max_latency=max_latency.getAndSet(0)
    )
    data_samples += data

    // we might need to increment number the producers..
    for (i <- 0 until producers_per_sample) {
      val client = createProducer(producer_clients.length)
      producer_clients ::= client
      client.start()
    }

    // we might need to increment number the consumers..
    for (i <- 0 until consumers_per_sample) {
      val client = createConsumer(consumer_clients.length)
      consumer_clients ::= client
      client.start()
    }

  }
  
  def createProducer(i:Int):Client
  def createConsumer(i:Int):Client

}


