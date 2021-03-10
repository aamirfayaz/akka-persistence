package learning.advanced_patterns

import akka.NotUsed
import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.journal.{Tagged, WriteEventAdapter}
import akka.persistence.query.{EventEnvelope, PersistenceQuery, Offset}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.DurationInt
import scala.util.Random

/**
  * Persistent stores are also used for reading data.
  * So event sourcing is powerful, allows not only to query current state of things, but also what the state
  * of a given state was at any given time.
  * Queries that it supports are:
  * - select persistence IDs
  * - select events by persistence IDs
  * - select events across persistenceIDs, by tags
  * These queries are used in practices for number of Use Cases:
  * - know which persistent actors are active in our system at any given time.
  * - might want to replay events by a persistentID i.e events written by an actor to recreate older state or
  * track how we arrived to the current state of the actor.
  * - data aggregation or data processing on events across persistenceIDs from the entire store.
  */

object PersistenceQueryDemo extends App {

  val system = ActorSystem("PersistenceQueryDemo", ConfigFactory.load().getConfig("persistenceQuery"))

  //going to access persistence query API directly, using read journal

  val readJournal: CassandraReadJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  //we can now access akka persistence api on this object
  //order is maintained i.e gives back IDs in the same order they were persisted.
  val persistenceIds: Source[String, NotUsed] = readJournal.persistenceIds() // get me all persistence IDs available in the journal

  val finiteStream: Source[String, NotUsed] = readJournal.currentPersistenceIds() // fetches and stream closes.

  implicit val materializer = ActorMaterializer()(system)

  //its cool, a stream, its live, means if any other persistent Id gets added to journal it will instantly display it.
  //this is fully infinte akka stream
  /*  persistenceIds.runForeach { persistenceId =>
      println(s"Found persistence ID: $persistenceId")
    }*/

  class SimplePersistentActor extends PersistentActor with ActorLogging {
    override def receiveRecover: Receive = {
      case e => log.info(s"Recovered: $e")
    }

    override def receiveCommand: Receive = {
      case m => persist(m) { _ =>
        log.info(s"persisted: ${m}")
      }
    }

    override def persistenceId: String = "persistence-query-id-1"
  }

/*  val simpleActor = system.actorOf(Props[SimplePersistentActor], "SimplePersistentActor")

  import system.dispatcher

  system.scheduler.scheduleOnce(8.seconds) {
    simpleActor ! "hello a third time time"
  }*/

  //fetch events by persistence ID

  //infinite streams too, live version
  val events: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("persistence-query-id-1", 0, Long.MaxValue)
  //events.runForeach(event => println(s"Read event: $event"))

  //finite or not live version
  val finiteEvents: Source[EventEnvelope, NotUsed] = readJournal.currentEventsByPersistenceId("persistence-query-id-1", 0, Long.MaxValue)

  //events by tags, allows us to query for events across multiple persistent IDs.

  //Music store online, Can buy PlayList of songs of different genres

  val genres: Array[String] = Array("pop", "rock", "hip-hop", "jazz", "disco")

  case class Song(artist: String, title: String, genre: String)

  //command
  case class PlayList(songs: List[Song])

  //event
  case class PlayListPurchased(id: Int, songs: List[Song])

  class MusicStoreCheckoutActor extends PersistentActor with ActorLogging {

    var latestPlayListId = 0

    override def receiveRecover: Receive = {
      case event@PlayListPurchased(id, _) =>
        log.info(s"Recovered: $event")
        latestPlayListId = id
    }

    override def receiveCommand: Receive = {
      case PlayList(songs) =>
        persist(PlayListPurchased(latestPlayListId, songs)) { e =>
          log.info(s"User purchased: $songs")
          latestPlayListId += 1
        }
    }

    override def persistenceId: String = "music-store-checkout"
  }

  class MusicStoreEventAdapter extends WriteEventAdapter {
    override def manifest(event: Any): String = "musicStore"

    override def toJournal(event: Any): Any = event match {
      case event@PlayListPurchased(_, songs) =>
        //intention is to tag this event by genre of the songs it contains
        // so later query eventByTag for a particular genre
        val genres = songs.map(_.genre).toSet
        Tagged(event, genres)
      case event => event
    }
  }

  val checkoutActor = system.actorOf(Props[MusicStoreCheckoutActor], "MusicStoreActor")

/*  val r = new Random()
  for (_ <- 1 to 10) { // total 10 playlists

    val maxSongs = r.nextInt(5) + 1 // 0 to 4 by random + 1
    val songs = for (i <- 1 to maxSongs) yield {
      val randomGenre = genres(r.nextInt(5))
      Song(s"Artist $i", s"My Love Song $i", randomGenre)
    }
    checkoutActor ! PlayList(songs.toList)

  }*/

  //playlist which contains rock songs, select all events by that tag
  /**
    * Documentation:
    * eventsByTag is used for retrieving events that were marked with a given tag, e.g. all events of an Aggregate Root type.
    */

    Thread.sleep(5000)

  //also infinite one, order is not maintained as it searches across persistentIDs
  val rockPlayList: Source[EventEnvelope, NotUsed] = readJournal.eventsByTag("jazz", Offset.noOffset)
  rockPlayList.runForeach { event =>
        println(s"Found a playlist with a rock song: $event")
      }

  //finite stream
  val rockPlayListFinite: Source[EventEnvelope, NotUsed] = readJournal.currentEventsByTag("jazz", Offset.noOffset)

}