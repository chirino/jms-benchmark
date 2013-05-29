package org.fusesource.jmsbenchmark

import javax.jms.{Destination, ConnectionFactory}
import org.fusesource.stomp.jms.{StompJmsTopic, StompJmsQueue, StompJmsConnectionFactory}

/**
 * <p>
 * ActiveMQ implementation of the JMS Scenario class.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class StompScenario extends JMSClientScenario {

  override protected def factory:ConnectionFactory = {
    allow_worker_interrupt = true
    val rc = new StompJmsConnectionFactory
    rc.setBrokerURI(url)
    rc.setOmitHost(true)
    rc
  }

  override protected def destination(i:Int):Destination = destination_type match {
    case "queue" => new StompJmsQueue("/queue/", indexed_destination_name(i))
    case "topic" => new StompJmsTopic("/topic/", indexed_destination_name(i))
    case _ => sys.error("Unsuported destination type: "+destination_type)
  }

}

object StompScenario {
  def main(args:Array[String]):Unit = {
    val scenario = new StompScenario
    scenario.url = "tcp://localhost:61613"
    scenario.display_errors = true
    scenario.user_name = "admin"
    scenario.password = "password"
    scenario.message_size = 20
    scenario.destination_type = "topic"
    scenario.producers = 1
    scenario.consumers = 1
    scenario.run()
  }
}