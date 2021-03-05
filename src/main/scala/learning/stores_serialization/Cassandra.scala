package learning.stores_serialization

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
  * Cassandra:
  * It's highly available.
  * great performance on writes and high throughput, beneficial for akka persistence, as we
     write events to cassandra all the time
  * Cassandra, some trade-offs we have to make in production, if we decide to use it.
  * First, we introduce eventual consistency in our systems, i.e
    If we write a value to Cassandra, then one of the nodes will be the first to write it, and
    other nodes will have to wait a certain time before they are also updated, during this time,
    the sub-sequent reads to Cassandra maybe inconsistent, but that's not such a big problem in the
    context of akka persistence, because we do writes to the database, most of the time.

   * reference.conf file in github repo: https://github.com/akka/akka-persistence-cassandra,
     this file will contain default values which cassandra journal will use.

   [
     - docker exec -it cassandra cqlsh
      select * from akka.messages; journal table
      select * from akka_snapshot.snapshots;
    ]
  */
object Cassandra extends App {

  val cassandraActorSystem = ActorSystem("cassandraSystem", ConfigFactory.load().getConfig("cassandraDemo"))
  val persistentActor = cassandraActorSystem.actorOf(Props[SimplePersistentActor], "simplePersistentActor")

/*  for (i <- 1 to 10) {
    persistentActor ! s"I love Akka [$i]"
  }
  persistentActor ! "print"
  persistentActor ! "snap"

  for (i <- 11 to 20) {
    persistentActor ! s"I love Akka [$i]"
  }*/

}
