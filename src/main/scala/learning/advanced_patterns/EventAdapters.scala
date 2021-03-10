package learning.advanced_patterns

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.journal.{EventAdapter, EventSeq, ReadEventAdapter, WriteEventAdapter}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

/**
  * Schema Evolution or Schema Versioning:
  * - As our app evolves, we may need to change the events structure.
  * - It's not a trivial problem, because:
  * --> If we decided to do a schema change, we need to take into account the already persisted data, so what
  * do we do with already persisted data.
  * --> how do we persist new data with the new schema along with the old events with old schema?
  */

/**
  * Assume we are designing an online store for acoustic guitars
  */

/*object EventAdapters_1 extends App {

  val ACOUSTIC = "accoustic"
  val ELECTRIC = "electric"

  case class Guitar(id: String, model: String, make: String, guitarType: String = "acoustic")

  //command
  case class AddGuitar(guitar: Guitar, quantity: Int)

  //event
  case class GuitarAdded(guitarId: String, guitarModel: String, guitarMake: String, quantity: Int)

  case class GuitarAddedV2(guitarId: String, guitarModel: String, guitarMake: String, quantity: Int, guitarType: String)

  class InventoryManager extends PersistentActor with ActorLogging {

    val inventory: mutable.Map[Guitar, Int] = new mutable.HashMap[Guitar, Int]()

    /**
      * With Schema Evolution, this will get very busy, so to overcome this akka has:
      * Event Adapters.
      */
    override def receiveRecover: Receive = {
      //have to live with them
      case event@GuitarAdded(id, model, make, quantity) =>
        log.info(s"Recovered $event")
        val guitar = Guitar(id, model, make)
        addGuitarInventory(guitar, quantity)
      //schema change, have to add all the time
      case event@GuitarAddedV2(id, model, make, quantity, guitarType) =>
        log.info(s"Recovered $event")
        val guitar = Guitar(id, model, make, guitarType)
        addGuitarInventory(guitar, quantity)
    }

    override def receiveCommand: Receive = {
      case AddGuitar(guitar@Guitar(id, model, make, guitarType), quantity) =>
        persist(GuitarAddedV2(id, model, make, quantity, guitarType)) { _ =>
          addGuitarInventory(guitar, quantity)
          log.info(s"Added $quantity x $guitar to inventory")
        }
      case "print" => log.info(s"Current inventory is $inventory")
    }

    def addGuitarInventory(guitar: Guitar, quantity: Int) = {
      val existingQuantity = inventory.getOrElse(guitar, 0)
      inventory.put(guitar, existingQuantity + quantity)
    }

    override def persistenceId: String = "guitar-inventory-manager"
  }

  val config = ConfigFactory.load().getConfig("eventAdapters")
  val system = ActorSystem("EventAdapters", config)
  val inventoryManager = system.actorOf(Props[InventoryManager], "InventoryManager")

  /*  val guitars = for (i <- 1 to 10) yield Guitar(s"$i", s"Hakker $i", "Aamir_the_autodidact")
    guitars foreach { guitar =>
      inventoryManager ! AddGuitar(guitar, 5)
    }*/

  inventoryManager ! "print"
}*/

//Event-Adapter
object EventAdapters extends App {

  val ACOUSTIC = "accoustic"
  val ELECTRIC = "electric"

  case class Guitar(id: String, model: String, make: String, guitarType: String = "acoustic")

  //command
  case class AddGuitar(guitar: Guitar, quantity: Int)

  //event
  case class GuitarAdded(guitarId: String, guitarModel: String, guitarMake: String, quantity: Int)

  case class GuitarAddedV2(guitarId: String, guitarModel: String, guitarMake: String, quantity: Int, guitarType: String)

  class InventoryManager extends PersistentActor with ActorLogging {

    val inventory: mutable.Map[Guitar, Int] = new mutable.HashMap[Guitar, Int]()

    override def receiveRecover: Receive = {
      case event@GuitarAddedV2(id, model, make, quantity, guitarType) =>
        log.info(s"Recovered $event")
        val guitar = Guitar(id, model, make, guitarType)
        addGuitarInventory(guitar, quantity)
    }

    override def receiveCommand: Receive = {
      case AddGuitar(guitar@Guitar(id, model, make, guitarType), quantity) =>
        persist(GuitarAddedV2(id, model, make, quantity, guitarType)) { _ =>
          addGuitarInventory(guitar, quantity)
          log.info(s"Added $quantity x $guitar to inventory")
        }
      case "print" => log.info(s"Current inventory is $inventory")
    }

    def addGuitarInventory(guitar: Guitar, quantity: Int) = {
      val existingQuantity = inventory.getOrElse(guitar, 0)
      inventory.put(guitar, existingQuantity + quantity)
    }

    override def persistenceId: String = "guitar-inventory-manager"
  }

  /**
    * A ReadEventAdapter is responsible for upcasting/converting events persisted in the journal to some other type
    *
    */
  class GuitarReadEventAdapter extends ReadEventAdapter {
    /** Flow:
      * from journal(bytes) -> serializer (deserialization) (to some class, old guitar event here) ->
      * read event adapter (to some other class based on this adapter) -> handle by actor in receiveRecover
      *
      * bytes -> GuitarAdded -> GuitarAddedV2 -> handle in receiveRecover
      */
    /**
      * event: Any is output of deserialization step.
      * manifest:String,  the manifest is a class name which serves as an indication for the serializer to know
        which class to instantiate for the object it's deserializing.
      * EventSeq: seq of events that the actor will handle in turn.
      */
    override def fromJournal(event: Any, manifest: String): EventSeq = event match {
      case GuitarAdded(id, model, make, quantity) =>
        EventSeq.single(GuitarAddedV2(id, model, make, quantity, ACOUSTIC))
      case other => EventSeq.single(other)
    }
  }

  //usually used for backwards compatibility
  class GuitarWriteEventAdapter extends WriteEventAdapter {

    // actor -> write event adapter, this only -> serializer -> journal
    override def manifest(event: Any): String = ???

    override def toJournal(event: Any): Any = ???
  }
  //Also, for both instead of code duplication
  /**
    * Event Adapter: sole job is to convert all the events to the latest version of event.
    */
  class GuitarEventAdaptr extends EventAdapter {
    override def manifest(event: Any): String = ???

    override def toJournal(event: Any): Any = ???

    override def fromJournal(event: Any, manifest: String): EventSeq = ???
  }

  val config = ConfigFactory.load().getConfig("eventAdapters")
  val system = ActorSystem("EventAdapters", config)
  val inventoryManager = system.actorOf(Props[InventoryManager], "InventoryManager")

    val guitars = for (i <- 1 to 10) yield Guitar(s"$i", s"Hakker $i", "Aamir_the_autodidact")
    guitars foreach { guitar =>
      inventoryManager ! AddGuitar(guitar, 5)
    }

  inventoryManager ! "print"
}
/**
These event adapters are only for journals (Events) and not for snapshots.
If i change the datamodel for the objects that are being saved in the snapshots,
what is my option if i have to recover from an older snapshot (snapshots that were created before i changed the model).
OR my only option is, to read the events and ignore the  older snapshots in order to recover?
When i start to recover from snapshot and i realize they are broken, can i programatically determine this,
and switch to reading from journals (events)? Is it possible to do this programatically?

There is no schema evolution for snapshots since you can store any state. My personal preference is either

- to delete snapshots when you are storing another snapshot of a different structure
- to maintain "schema" (read: structure) evolution for snapshots yourself by managing all historical classes
   if you simply cannot rely on a single snapshot and it's important that you do
 */