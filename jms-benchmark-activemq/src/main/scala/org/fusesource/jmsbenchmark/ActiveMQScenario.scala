package org.fusesource.jmsbenchmark

import org.apache.activemq.ActiveMQConnectionFactory
import javax.jms.{Destination, ConnectionFactory}
import org.apache.activemq.command.{ActiveMQTopic, ActiveMQQueue}
import management.ManagementFactory
import javax.management.ObjectName
import javax.management.openmbean.CompositeData

object ActiveMQScenario {
  def main(args:Array[String]):Unit = {
    val scenario = new ActiveMQScenario
    scenario.url = "tcp://mac-pro:61613"
    scenario.display_errors = true
    scenario.user_name = "admin"
    scenario.password = "password"
    scenario.message_size = 1000
    scenario.producers = 100
    scenario.consumers = 100
    scenario.persistent = true
    scenario.tx_size = 10
    scenario.destination_type = "queue"
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

  var use_async_send_callback = java.lang.Boolean.getBoolean("use_async_send_callback")

  override protected def factory:ConnectionFactory = {
    val rc = new ActiveMQConnectionFactory
    rc.setBrokerURL(url)
    rc.setCloseTimeout(3*1000)

    // Lets optimize the prefetch used for the scenario.
    val mbean_server = ManagementFactory.getPlatformMBeanServer()
    val heap_usage = mbean_server.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage").asInstanceOf[CompositeData]
    val heap_max = heap_usage.get("max").asInstanceOf[java.lang.Long].longValue()

    val prefetch_available_heap = (heap_max-(1024*1024*500))/10
    if ( consumers*message_size > 0 ) {
      val prefech_size = prefetch_available_heap/(consumers*message_size)
      if( prefech_size < 1000 ) {
        rc.getPrefetchPolicy.setAll(prefech_size.toInt)
      }
    }

    rc
  }

  val send_callback = new AsyncCallback(){
    def onException(error: JMSException) {
      if( !done.get() && display_errors ) {
        error.printStackTrace()
      }
    }
    def onSuccess() {
      // An app could use this to know if the send succeeded.
    }
  }

  def send_message(producer: MessageProducer, msg: TextMessage) {
    if( use_async_send_callback && persistent && tx_size==0 ) {
      ((ActiveMQMessageProducer)producer).send(msg, send_callback)
    } else {
      super.send_message()
    }
  }


  override protected def destination(i:Int):Destination = destination_type match {
    case "queue" => new ActiveMQQueue(indexed_destination_name(i))
    case "topic" => new ActiveMQTopic(indexed_destination_name(i))
    case _ => sys.error("Unsuported destination type: "+destination_type)
  }

}