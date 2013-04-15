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

import java.lang.Thread
import javax.jms._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CyclicBarrier

/**
 * <p>
 * Simulates load on a JMS sever using the JMS messaging API.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract class JMSClientScenario extends Scenario {

  var allow_worker_interrupt = false

  def createProducer(i:Int) = {
    new ProducerClient(i)
  }
  def createConsumer(i:Int) = {
    new ConsumerClient(i)
  }

  protected def destination(i:Int):Destination

  def indexed_destination_name(i:Int) = destination_type match {
    case "queue" => queue_prefix+destination_name+"-"+(i%destination_count)
    case "topic" => topic_prefix+destination_name+"-"+(i%destination_count)
    case _ => sys.error("Unsuported destination type: "+destination_type)
  }


  protected def factory:ConnectionFactory

  def jms_ack_mode = {
    ack_mode match {
      case "auto" => Session.AUTO_ACKNOWLEDGE
      case "client" => Session.CLIENT_ACKNOWLEDGE
      case "dups_ok" => Session.DUPS_OK_ACKNOWLEDGE
      case "transacted" => Session.SESSION_TRANSACTED
      case _ => throw new Exception("Invalid ack mode: "+ack_mode)
    }
  }

  def load_start_rendezvous(client:JMSClient, session:Session) = {
    if ( connected_latch !=null ) {
      connected_latch.countDown()
    }
    if ( apply_load_latch !=null ) {
      apply_load_latch.await()
    }
  }

  trait JMSClient extends Client {

    @volatile
    var connection:Connection = _
    var message_counter=0L
    val done = new AtomicBoolean()

    var worker:Thread = _

    var close_thread:Thread = null
    def dispose {
      if( close_thread==null ) {
        close_thread = new Thread(name+" closer") {
          override def run() {
            try {
              connection.close()
            } catch {
              case _ =>
            }
          }
        }
        close_thread.start()
      }
    }

    def execute:Unit

    def start = {
      worker = new Thread(name+" worker") {
        override def run() {
          var reconnect_delay = 0
          while( !done.get ) {
            try {

              if( reconnect_delay!=0 ) {
                Thread.sleep(reconnect_delay)
                reconnect_delay=0
              }
              connection = factory.createConnection(user_name, password)
              if( durable ) {
                connection.setClientID(name)
              }
              connection.setExceptionListener(new ExceptionListener {
                def onException(exception: JMSException) {
                }
              })
              connection.start()

              execute

            } catch {
              case e:Throwable =>
                if( !done.get ) {
                  if( display_errors ) {
                    e.printStackTrace
                  }
                  error_counter.incrementAndGet
                  reconnect_delay = 1000
                }
            } finally {
              dispose
            }
          }
        }
      }
      worker.start
    }

    def shutdown = {
      done.set(true)
      dispose
    }

    def wait_for_shutdown = {
      if ( worker!=null ) {
        val start = System.currentTimeMillis();
        worker.join(6000)
        while(worker.isAlive ) {
          if( allow_worker_interrupt ) {
            println(name+" worker did not shutdown quickly.. interrupting thread.")
            worker.interrupt()
            worker.join(1000)
          } else {
            println(name+" worker did not shutdown quickly...")
            worker.join(1000)
          }
        }
        if( close_thread!=null ) {
          val wait = 6000 - (System.currentTimeMillis() - start)
          if( wait > 0 ) {
            close_thread.join(wait)
          }
          while(close_thread.isAlive ) {
            if( allow_worker_interrupt ) {
              println(name+" closing thread did not shutdown quickly.. interrupting thread.")
              close_thread.interrupt()
              close_thread.join(1000)
            } else {
              println(name+" closing thread did not shutdown quickly...")
              close_thread.join(1000)
            }
          }
          close_thread.join()
        }
        worker = null
      }
    }
    def name:String
  }

  class ConsumerClient(override val id: Int) extends JMSClient {
    def name = "consumer " + id

    def execute {
      var session = if(tx_size==0) {
        connection.createSession(false, jms_ack_mode)
      } else {
        connection.createSession(true, Session.SESSION_TRANSACTED)
      }

      var consumer = if( durable ) {
        session.createDurableSubscriber(destination(id).asInstanceOf[Topic], name, selector, no_local)
      } else {
        session.createConsumer(destination(id), selector, no_local)
      }

      load_start_rendezvous(this, session)

      var tx_counter = 0
      while( !done.get() ) {
        val msg = consumer.receive(500)
        if( msg!=null ) {
          val latency = System.nanoTime() - msg.getLongProperty("ts")
          update_max_latency(latency)
          consumer_counter.incrementAndGet()

          val sleep  = _consumer_sleep(this)
          if (sleep != 0) {
            Thread.sleep(sleep)
          }
          if(session.getAcknowledgeMode == Session.CLIENT_ACKNOWLEDGE) {
            msg.acknowledge();
          }

          if ( tx_size != 0 ) {
            tx_counter += 1
            if ( tx_counter == tx_size) {
              session.commit()
              tx_counter = 0
            }
          }

        } else {
          // commit early once we don't get anymore messages.
          if ( tx_size != 0 && tx_counter > 0 ) {
            session.commit()
            tx_counter = 0
          }
        }

      }
    }

  }

  class ProducerClient(override val id: Int) extends JMSClient {

    def name = "producer " + id

    def execute {

      var session = if(tx_size==0) {
        connection.createSession(false, jms_ack_mode)
      } else {
        connection.createSession(true, Session.SESSION_TRANSACTED)
      }

      val producer:MessageProducer = session.createProducer(destination(id))
      producer.setDeliveryMode(if( persistent ) {
        DeliveryMode.PERSISTENT
      } else {
        DeliveryMode.NON_PERSISTENT
      })

      val msg = session.createTextMessage(body(name))
      headers_for(id).foreach { case (key, value) =>
        msg.setStringProperty(key, value)
      }

      load_start_rendezvous(this, session)

      var tx_counter = 0
      while( !done.get() ) {
        msg.setLongProperty("ts", System.nanoTime());
        producer.send(msg)
        producer_counter.incrementAndGet()

        val sleep = _producer_sleep(this)
        if (sleep != 0) {
          Thread.sleep(sleep)
        }

        if ( tx_size != 0 ) {
          tx_counter += 1
          if ( tx_counter == tx_size) {
            session.commit()
            tx_counter = 0
          }
        }
      }

    }
  }

  def body(name:String) = {
    val buffer = new StringBuffer(message_size)
    buffer.append("Message from " + name+"\n")
    for( i <- buffer.length to message_size ) {
      buffer.append(('a'+(i%26)).toChar)
    }
    var rc = buffer.toString
    if( rc.length > message_size ) {
      rc.substring(0, message_size)
    } else {
      rc
    }
  }



}
