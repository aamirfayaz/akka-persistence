package learning.eventsourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

import java.util.Date
import scala.collection.immutable

object PersistentActors1 extends App {

  //command for accountant_shehzal_corp (we as programmers sent)
  case class Invoice(recipient: String, date: Date, amount: Int)

  //Events (to journal)
  case class InvoiceRecipient(id: Int, recipient: String, date: Date, amount: Int)

  class Accountant_Shehzal_Corp extends PersistentActor with ActorLogging {

    var latestInvoiceId = 0
    var totalAmount = 0

    /**
      * called on recovery i.e when an actor starts or is restarted as a result of a supervisor strategy.
      * On recovery, this actor will query the persistent store for all events associated with this persistentId,
      * and all the events will be replayed by this actor in the same order that they were originally persisted.
      * Those events from store during recovery will be sent to this actor as simple messages, and this,
      * receiveRecover handler will handle them.
      * Recreating the state of the persistent actor
      */
    override def receiveRecover: Receive = {
      /**
        * Best Practice: follow the same state change that we did  in the persist/handler steps of receiveCommand
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
        * you can  persist an event in the store
        * you persist the event asynchronously, then pass in a callback, that will be written once the event is written
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

  }
      /**
        * we are not obliged to call or define persist method here in receive method, we can act like a normal actor
        */
          case "print" =>
           log.info(s"Latest invoice id: $latestInvoiceId, total amount: $totalAmount")
        }


    /**
      * is the way, events persisted by this actor, will be identified in the persistent store.
      * Unique per actor is the best practices otherwise it's not enforced by akka
      */
    override def persistenceId: String = "shehzal_corp_accountant"

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

object PersistentActors2 extends App {
  /**
    * Persistence Failures: 2 types to discuss
    * Call to persist() method fails i.e call to persist throws an error
    * journal implementation fails to persist a message
    * Hard to test, duh
    */

  case class Invoice(recipient: String, date: Date, amount: Int)
  case class InvoiceBulk(invoices: List[Invoice])
  case class InvoiceRecipient(id: Int, recipient: String, date: Date, amount: Int)
  case object ShutDown

  class Accountant_Shehzal_Corp extends PersistentActor with ActorLogging {
    var latestInvoiceId = 0
    var totalAmount = 0

    /**
      *  the point of recovery is to reconstruct your internal data (inside the actor/actors) in case something goes wrong or the system goes down.
    */

    override def receiveRecover: Receive = {
      case InvoiceRecipient(id, _, _, amount) =>
        latestInvoiceId = id
        totalAmount += amount
        log.info(s"Recovered invoice for #$id, for amount: $amount, total-amount: $totalAmount")
    }
    override def receiveCommand: Receive = {

      case ShutDown => context.stop(self) // will be picked from mailbox at the end, if sent at last

      case InvoiceBulk(invoices) =>
        val invoiceIds = latestInvoiceId to (latestInvoiceId + invoices.size)
        val events: immutable.Seq[InvoiceRecipient] = invoiceIds.zip(invoices).map { pair =>
          val id = pair._1
          val invoice = pair._2
          InvoiceRecipient(id, invoice.recipient, invoice.date, invoice.amount)
        }
          /**
            *  Asynchronously persists events in specified order. This is equivalent to calling persist[A](event: A)(handler: A => Unit)
            *  multiple times with the ["same handler"], except that events are persisted atomically with this method.
            *  callback/handler executed after each event has been persisted to the journal
           */
        persistAll(events) { e =>
            latestInvoiceId += 1
             totalAmount += e.amount
          log.info(s"Persisted Single $e as invoice #${e.id}, for total amount: $totalAmount")
        }
      case Invoice(recipient, data, amount) =>
        log.info(s"Received Invoice  for amount: $amount")
        val event = InvoiceRecipient(latestInvoiceId, recipient, data, amount)
        persist(event){ event =>
          latestInvoiceId += 1
          totalAmount += amount
          log.info(s"Persisted $event as invoice #${event.id}, for total amount: $totalAmount")

  }
          case "print" =>
           log.info(s"Latest invoice id: $latestInvoiceId, total amount: $totalAmount")
        }
    override def persistenceId: String = "shehzal_corp_accountant"

    /***
      * The Persistent Actor is always stopped after this method has been invoked.
      * Best Practice: start the actor again after a while, use BackOff Supervisor
      * event:Any is, failed to persist the event
      * seqNr:Long of event:Any from journal's point of view
      * From api:
      * Called when persist fails. By default it logs the error.
      * Subclass may override to customize logging and for example send negative acknowledgment to sender.
      * The actor is always stopped after this method has been invoked.
      * Note that the event may or may not have been saved, depending on the type of failure.
      * */
    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Failed to persist event: $event because of: $cause")
      super.onPersistFailure(cause, event, seqNr)
    }

    /**
      * Called if the journal fails to persist the event.
      * In this case, actor is Resumed , not stopped :-), coz we know for that the event was not persisted, and so
      * the actor state was not corrupted in the process.
      * from api:
      * Called when the journal rejected persist of an event. The event was not stored.
      * By default this method logs the problem as an error, and the actor continues.
      * The callback handler that was passed to the persist method will not be invoked.
      * */
    override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Persist reject for event: $event because of: $cause")
      super.onPersistRejected(cause, event, seqNr)
    }

  }

  val system = ActorSystem("PersistentActor")
  val accountant_shehzal_crop = system.actorOf(Props[Accountant_Shehzal_Corp], "Accountant_Shehzal_Corp")

 //testing single event case, test #1
  for (i <- 1 to 10) {
    accountant_shehzal_crop ! Invoice("shehzal_software", new Date(), i * 1000)
  }


  /**
    * Persisting multiple events -> using persistAll
    * */
  //testing bulk event case
 /* val invoices = for(i <- 1 to 5) yield Invoice("shehzal corp", new Date, i * 1000)
  accountant_shehzal_crop ! InvoiceBulk(invoices.toList)*/

  /**
     * Never ever call persist or persistAll from Futures, otherwise we risk breaking actor encapsulation, risk
    * corrupting actor state
    */

  /**
    * Shutdown of persistent actors:
    * PoisonPill or normal shutdown methods of actors not suggested to shutdown persistent actors,
    * as persistent actor might still be un-stashing some events and we know that PoisonPill or kill messages
    * executes in a separate mailbox, so risk of losing events from those.
    */

    //accountant_shehzal_crop ! PoisonPill // uncommented test #1 and wait for surprise
  /**
    * Best practices: Define your own "ShutDown" messages
    */

  accountant_shehzal_crop ! ShutDown // at last of mailbox

  //check if actor was shutdown
  Thread.sleep(5000)
  println("\n---------")

  accountant_shehzal_crop ! "print"
}

/**
  * Q: Akka is supposed to help us in making systems which are non-blocking on resources,
       so that requests are processed without wait.
  * Lecture comment
  * case Invoice(recipient, date, amount) =>

  persist(InvoiceRecorded(latestInvoiceId, recipient, date, amount))
  { e =>
    log.info(s"Persisted $e as invoice #${e.id}, for total amount $totalAmount")
  }
  //thread can do other stuff here, but i would not change the state of the actor here, no mutations,
   because you don't know when the handler will run.


  * In this case statement, the persist call is asynchronous. So once the persist call is made,
  * the dispatcher thread that is processing this command, is free to do other stuff,
  * (please see the comment in the code above) you can write more code after the handler section.

  * Also if there are other messages arriving in the mailbox, they get stashed.

  */

/**
  *The thread in charge of the actor may or may not release control of the actor in between persist and the subsequent handler,
  *  you don't control that. However, in between persist and the handler the actor state is safe -
  *  as long as you don't break actor encapsulation entirely, which is a problem way beyond persistence.
  */
