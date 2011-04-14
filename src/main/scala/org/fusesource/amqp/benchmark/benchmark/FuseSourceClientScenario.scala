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
package org.fusesource.amqp.benchmark

import java.io._
import org.fusesource.hawtdispatch._
import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.fusesource.fusemq.amqp.api._
import org.fusesource.fusemq.amqp.codec.types.TypeFactory._
import org.fusesource.hawtbuf.Buffer._

/**
 * <p>
 * Simulates load on the an AMQP sever using the FuseSource
 * AMQP client library.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class FuseSourceClientScenario extends Scenario {

  def createProducer(i:Int) = {
    new ProducerClient(i)
  }
  def createConsumer(i:Int) = {
    new ConsumerClient(i)
  }

  trait FuseSourceClient extends Client {

    var connection = AmqpConnectionFactory.create

    def queue = connection.getDispatchQueue

    var message_counter=0L
    var reconnect_delay = 0L

    sealed trait State

    case class INIT() extends State



    case class CONNECTING(host: String, port: Int, on_complete: ()=>Unit) extends State {


      def connect() = {
        connection = AmqpConnectionFactory.create
        connection.setOnClose(^{on_close})
        connection.connect("tcp://" + host + ":" + port, ^ {
          if ( this == state ) {
            if(done.get) {
              close
            } else {
              if( connection.connected ) {
                state = CONNECTED()
                on_complete()
              } else {
                on_failure(connection.error)
              }
            }
          }
        })

        // Times out the connect after 5 seconds...
        queue.after(5, TimeUnit.SECONDS) {
          if ( this == state ) {
            on_failure(new Exception("Connect timed out"))
          }
        }
      }

      // We may need to delay the connection attempt.
      if( reconnect_delay==0 ) {
        connect
      } else {
        queue.after(1, TimeUnit.SECONDS) {
          if ( this == state ) {
            reconnect_delay=0
            connect
          }
        }
      }

      def close() = {
        if( connection.connected ) {
          connection.close
          state = CLOSING()
        } else {
          state = DISCONNECTED()
        }
      }

      def on_failure(e:Throwable) = {
        if( display_errors ) {
          e.printStackTrace
        }
        error_counter.incrementAndGet
        reconnect_delay = 1000
        close
      }

    }

    case class CONNECTED() extends State {

      def close() = {
        if( connection.connected ) {
          connection.close
          state = CLOSING()
        } else {
          state = DISCONNECTED()
        }
      }

      def on_failure(e:Throwable) = {
        if( display_errors ) {
          e.printStackTrace
        }
        error_counter.incrementAndGet
        reconnect_delay = 1000
        close
      }

    }
    case class CLOSING() extends State

    case class DISCONNECTED() extends State {
      queue {
        if( state==this ){
          if( done.get ) {
            has_shutdown.countDown
          } else {
            reconnect_action
          }
        }
      }
    }


    def on_close:Unit = {
      if( done.get ) {
        has_shutdown.countDown
      } else {
        if( connection.error !=null ) {
          state match {
            case x:CONNECTING => x.on_failure(connection.error)
            case x:CONNECTED => x.on_failure(connection.error)
            case _ =>
          }
        } else {
          state = DISCONNECTED()
        }
      }
    }


    var state:State = INIT()

    val has_shutdown = new CountDownLatch(1)
    def reconnect_action:Unit

    def start = queue {
      state = DISCONNECTED()
    }

    def queue_check = assert(getCurrentQueue == queue)

    def open(host: String, port: Int)(on_complete: =>Unit) = {
      queue_check
      assert ( state.isInstanceOf[DISCONNECTED] )
      state = CONNECTING(host, port, ()=>on_complete)
    }

    def close() = {
      queue_check
      state match {
        case x:CONNECTING => x.close
        case x:CONNECTED => x.close
        case _ =>
      }
    }

    def shutdown = {
      assert(done.get)
      queue {
        close
      }
      has_shutdown.await()
    }

    def connect(proc: =>Unit) = {
//      println(name+" connecting..")
      queue_check
      if( !done.get ) {
        open(host, port) {
//          println(name+" connected..")
          proc
        }
      }
    }

    def name:String
  }

  class ConsumerClient(val id: Int) extends FuseSourceClient {
    val name: String = "consumer " + id
    queue.setLabel(name)

    var session:Session = _
    var receiver:Receiver = _

    override def reconnect_action = {
      connect {
        session = connection.createSession
        session.begin(^{
//          println(name+" session created")

          receiver = session.createReceiver
          receiver.setName(name+" - receiver")
          receiver.enableFlowControl(true)
          receiver.setAddress(destination(id))
          receiver.setListener(new MessageListener {

            var sleeping = false
            var _refiller = ^{}

            def needLinkCredit(available: Long) = available.max(1)
            def refiller(refiller: Runnable) = _refiller = refiller

            def full = sleeping
            def offer(receiver: Receiver, message: Message) = {

              if( sleeping ) {
                false
              } else {

                def settle = {
                  if ( !message.getSettled ) {
                    receiver.settle(message, Outcome.ACCEPTED)
                  }
                }

                val c_sleep = consumer_sleep
                if( c_sleep != 0 ) {
                  sleeping = true
                  queue.after(math.abs(c_sleep), TimeUnit.MILLISECONDS) {
                    sleeping = false
                    settle
                    _refiller.run
                  }
                } else {
                  settle
                }

                consumer_counter.incrementAndGet()
                true
              }
            }
          })
          receiver.attach(^{
//            println(name+" receiver attached")

          })
        })
      }
    }


  }

  class ProducerClient(val id: Int) extends FuseSourceClient {

    val name: String = "producer " + id
    queue.setLabel(name)

    var session:Session = _
    var sender:Sender = _

    override def reconnect_action = {
      connect {
        session = connection.createSession
        session.begin(^ {

//          println(name+" session created..")
          sender = session.createSender
          sender.setName(name + "sender")
          sender.setAddress(destination(id))
          sender.attach(^{
//            println(name+" sender created..")
            send_next
          })
        })

      }
    }

    def send_next:Unit = {
      val message = sender.createMessage
      message.setSettled(!sync_send)
      message.getHeader.setDurable(durable)

      if( !headers.isEmpty ) {
        val attrs = createAmqpMessageAttributes
        headers_for(id).foreach { case (key,value) =>
          attrs.put(createAmqpSymbol(key), createAmqpString(value))
        }
        message.getHeader.setMessageAttrs(attrs)
      }

      message.addBodyPart(ascii(body(name)))



      def send_completed:Unit = {
        message_counter += 1
        producer_counter.incrementAndGet()

        def continue_sending = {
          if(messages_per_connection > 0 && message_counter >= messages_per_connection  ) {
            message_counter = 0
            close
          } else {
            send_next
          }
        }

        val p_sleep = producer_sleep
        if(p_sleep != 0) {
          queue.after(math.abs(p_sleep), TimeUnit.MILLISECONDS) {
            continue_sending
          }
        } else {
          continue_sending
        }
      }

      def send:Unit = if( !done.get) {
        if( sync_send ) {
          message.onAck(^ {
            message.getOutcome match {
              case Outcome.ACCEPTED =>
                send_completed

              case _ =>
                // try again...
                message.setSettled(false)
                send
            }
          })

        } else {
          message.onSend(^{
            send_completed
          })
        }
        sender.put(message)
      }

      if( !done.get ) {
        send
      } else {
        close
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
