package org.fusesource.jmsbenchmark

import org.apache.activemq.ActiveMQConnectionFactory
import javax.jms.{Destination, ConnectionFactory}
import org.apache.activemq.command.{ActiveMQTopic, ActiveMQQueue}

object ActiveMQScenario {
  def main(args:Array[String]):Unit = {
    val scenario = new ActiveMQScenario
    scenario.url = "tcp://localhost:61613"
    scenario.display_errors = true
    scenario.user_name = "admin"
    scenario.password = "password"
    scenario.message_size = 20
    scenario.producers = 1
    scenario.consumers = 0
    scenario.destination_type = "topic"
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

  override protected def factory:ConnectionFactory = {
    val rc = new ActiveMQConnectionFactory
    rc.setBrokerURL(url)
    rc
  }

  override protected def destination(i:Int):Destination = destination_type match {
    case "queue" => new ActiveMQQueue(indexed_destination_name(i))
    case "topic" => new ActiveMQTopic(indexed_destination_name(i))
    case _ => sys.error("Unsuported destination type: "+destination_type)
  }

}