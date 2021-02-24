package learning

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

import java.util.Date

object PersistentActors extends App {

  //command for accountant_shehzal_corp (we as programmers sent)
  case class Invoice(recipient: String, date: Date, amount: Int)

  //Events (to journal)
  case class InvoiceRecipient(id: Int, recipient: String, date: Date, amount: Int)

  class Accountant_Shehzal_Corp extends PersistentActor with ActorLogging {

    var latestInvoiceId = 0
    var totalAmount = 0

    /**
      * called on recovery i.e when an actor starts or is restarted as a result of a
      * supervisor strategy
      * On recovery, this actor will query the persistent store for all events associated with this persistentId,
      * and all the events will be replayed by this actor in the same order that they were originally persisted.
      * Those events from store during recovery will be sent to this actor as simple messages, and this,
      * receiveRecover handler will handle them.
      * Recreating the state of the persistent actor
      */
    override def receiveRecover: Receive = {
      /**
        * Best Practice: follow the logs in the persist/handler steps of receiveCommand
        */
      case InvoiceRecipient(id, _, _, amount) =>
        latestInvoiceId = id
        totalAmount += amount
        log.info(s"Recovered invoice for #$id, for amount: $amount, total-amount: $totalAmount")
    }

    /**
      * Then "normal" receive method
      */
    override def receiveCommand: Receive = {
      /**
        * when you create a command/send a command:
        * you can an event to persist in the store
        * you persist the even, asynchronously, then pass in a callback, that will be written once the event is written
        * Mostly in handler, we update the actor's state once the event is persisted, also asynchronously
        */
      case Invoice(recipient, data, amount) =>
        log.info(s"Received Invoice  for amount: $amount")
        val event = InvoiceRecipient(latestInvoiceId, recipient, data, amount)
        persist(event) /**time gap: all other messages send to this actor stashed*/{ event /*: InvoiceRecipient*/ =>

          /**
            * safe to access mutable state here, because from documentation:
            * It is guaranteed that no new commands will be received by a persistent actor
            * between a call to persist and the execution of its handler
            */
          latestInvoiceId += 1
          totalAmount += amount

          /**
            * We can correctly identify the sender of the command
            */
          //sender ! "PersistenceACK"
          log.info(s"Persisted $event as invoice #${event.id}, for total amount: $totalAmount")
      /**
      * is the way events persisted by this actor will be identified in the persistent store.
      * Unique per actor, best practices not enforced by akka
      */
          case "print" =>
           log.info(s"Latest invoice id: $latestInvoiceId, total amount: $totalAmount")
        }
    }


    override def persistenceId: String = "shehzal_corp_accountant"
    /**
    * we are not obliged to call or define persist method here, we can act like a normal actor
      */
  }

  /**
    * The way we construct an instance of this persistent actor is the way we normally construct an instance of an actor
    */
  val system = ActorSystem("PersistentActor")
  val accountant_shehzal_crop = system.actorOf(Props[Accountant_Shehzal_Corp], "Accountant_Shehzal_Corp")

  /**
    * Commenting out this part will make recoverCommand handler to be event and events will be replayed.
    */


  /*for (i <- 1 to 10) {
    accountant_shehzal_crop ! Invoice("shehzal_software", new Date(), i * 1000)
  }*/
}