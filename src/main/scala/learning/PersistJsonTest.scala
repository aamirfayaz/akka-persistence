package learning

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import com.typesafe.config.ConfigFactory

object PersistJsonTest extends App {

  class PersistJson extends PersistentActor with ActorLogging {

    override def receiveRecover: Receive = {
      case s: String =>
        log.info(s"Recovered $s")
    }

    override def receiveCommand: Receive = {
      case s: String =>
        persist(s) { e =>
          log.info(s"persisted: ${e}")
        }
    }

    override def persistenceId: String = "persist-json-test"
  }

  val system = ActorSystem("PersistJsonTestAS", ConfigFactory.load().getConfig("cassandraDemo"))
  val jsonPersistActor = system.actorOf(Props[PersistJson], "PersistJsonActor")
/*
  val messages =
    s"""
       |
       |""".stripMargin
  (1 to 5).foreach { i =>

    val message =
      s"""
         |{
         | "id" : ${i},
         | "name" : "Name-$i"
         | }
         |""".stripMargin
    jsonPersistActor ! message
  }*/

}
