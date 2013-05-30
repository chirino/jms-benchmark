package org.fusesource.jmsbenchmark

import org.apache.activemq._
import javax.jms._
import org.apache.activemq.command.{ActiveMQTopic, ActiveMQQueue}
import javax.management.ObjectName
import javax.management.openmbean.CompositeData
import java.lang.management.ManagementFactory
import java.util.concurrent.Semaphore

object ActiveMQScenario {
  def main(args:Array[String]):Unit = {
    val scenario = new org.fusesource.jmsbenchmark.ActiveMQScenario
    scenario.url = "tcp://localhost:61616"
    scenario.display_errors = true
    scenario.user_name = "admin"
    scenario.password = "password"
    scenario.message_size = 10
    scenario.producers = 1
    scenario.consumers = 0
    scenario.persistent = true
    scenario.tx_size = 0
    scenario.destination_type = "queue"
    scenario.destination_name = "load-"
    scenario.ack_mode="client"
    scenario.jms_bypass = true
    scenario.run()
  }
}

/**
 * <p>
 * ActiveMQ implementation of the JMS Scenario class.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ActiveMQScenario extends JMSClientScenario {

  var jms_bypass = java.lang.Boolean.getBoolean("jms_bypass")

  override protected def factory:ConnectionFactory = {
    val rc = new ActiveMQConnectionFactory
    rc.setBrokerURL(url)
    rc.setCloseTimeout(3*1000)

    // Lets optimize the prefetch used for the scenario.
    val mbean_server = ManagementFactory.getPlatformMBeanServer()
    val heap_usage = mbean_server.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage").asInstanceOf[CompositeData]
    val heap_max = heap_usage.get("max").asInstanceOf[java.lang.Long].longValue()

    val prefetch_available_heap = heap_max/10 // use 1/10 of the heap for prefetch.
    if ( prefetch_available_heap > 0 && consumers*message_size > 0 ) {
      val prefech_size = prefetch_available_heap/(consumers*message_size)
      rc.getPrefetchPolicy.setAll(prefech_size.toInt.min(100*1000))
    }

    send_semaphore = new Semaphore( (prefetch_available_heap/(producers*message_size)).min(Int.MaxValue).toInt )

    rc.setWatchTopicAdvisories(false)

    if( jms_bypass ) {
      use_message_listener = true
      rc.getPrefetchPolicy.setAll(1000*100)
      rc.setAlwaysSessionAsync(false)
      rc.setCheckForDuplicates(false)
    }

    rc
  }


  var send_semaphore:Semaphore = null

  val send_callback = new AsyncCallback(){
    def onException(error: JMSException) {
      if( !done.get() && display_errors ) {
        error.printStackTrace()
      }
      send_semaphore.release()
    }
    def onSuccess() {
      // An app could use this to know if the send succeeded.
      send_semaphore.release()
    }
  }

  override def send_message(producer: MessageProducer, msg: TextMessage) {
    if( jms_bypass && persistent && tx_size==0 ) {
      send_semaphore.acquire()
      producer.asInstanceOf[ActiveMQMessageProducer].send(msg, send_callback)
    } else {
      super.send_message(producer, msg)
    }
  }


  override protected def destination(i:Int):Destination = destination_type match {
    case "queue" => new ActiveMQQueue(indexed_destination_name(i))
    case "topic" => new ActiveMQTopic(indexed_destination_name(i))
    case _ => sys.error("Unsuported destination type: "+destination_type)
  }

}