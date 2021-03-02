package learning

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.{PersistentActor, Recovery, RecoveryCompleted, SnapshotSelectionCriteria}

case class Command(contents: String)

case class Event(id: Int, contents: String)

class RecoveryActor extends PersistentActor with ActorLogging {

  override def persistenceId: String = "recovery-actor"

  def online(latestPersistedId: Int): Receive = {
    case Command(contents) =>
      println(s"latestpers id: $latestPersistedId")
      persist(Event(latestPersistedId, contents)) { event =>
        log.info(s"Successfully persisted $event, recovery is ${if (this.recoveryFinished) "" else "Not"} finished")
        context.become(online(latestPersistedId + 1))
      }
  }

  override def receiveCommand: Receive = online(1)


  override def receiveRecover: Receive = {

    case RecoveryCompleted =>
      log.info(s" I have finished recovery")
    //useful, additional initialization here, that will be critical to actor and done after recovery
    case Event(id, contents) =>
      //if(contents.contains("314")) throw new Exception("I can't take this anymore")
      log.info(s"Recovered: $contents,, recovery is ${if (this.recoveryFinished) "" else "Not"} finished")

      /**
        * This will not change the event handler during recovery,
        * it will always be receiveRecover during recovery
        * Also, after recovery, the "normal" handler will be the result of all the stacking of context.becomes
          i.e receiveCommand with latest id with make online as handler once done with recovery
        */
      context.become(online(id + 1)) // not good in production, in practice, we might not have relevant data

  }

  /**
    * called where there is a failure during receiveRecover
    *
    * @param cause , thrown during recovery
    * @param event : which was attempted to recover
    */
  override def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
    log.error(s"I failed at recovery")
    super.onRecoveryFailure(cause, event)
  }

  //override def recovery: Recovery = Recovery(toSequenceNr = 100) // mostly for debugging purposes
  //override def recovery: Recovery = Recovery(fromSnapshot = SnapshotSelectionCriteria.None)
  /**
    * Recovery will not take place, i.e disable recovery
    */
  // override def recovery: Recovery = Recovery.none

}

object RecoveryActorApp extends App {
  val system = ActorSystem("RecoveryDemo")
  val recoveryActor = system.actorOf(Props[RecoveryActor], "recoveryActor")

   /* for (i <- 1 to 1000) {
      recoveryActor ! Command(s"command $i")
    }*/
  // ALL COMMANDS SENT DURING RECOVERY ARE STASHED

  /**
    * 1- failure during recovery:
    * onRecoveryFailure + actor is stopped (unconditional, as actor can't be trusted anymore, inconsistent state)
    * check private val recoveryBehavior: Receive = { in Eventsourced.scala
    */

  /** 2-
    * Customizing recovery, to recover events up-to a certain sequence number using def recovery
    * - Please don't persist more events after a customized incomplete recovery
    */

  /** 3-
    * Recovery Status: Knowing when you are done recovery
    * - this.recoveryFinished, make sense if its present in block of code which is shared between
    * receiveCommand and receiveRecovery, there it makes sense to differentiate the logic between the two
    * 4-Getting a signal when we are done recovering, that will be a special message
    * - RecoveryCompleted
    */

  /** 5-
    * Stateless actors
    */

  Thread.sleep(10000)
  recoveryActor ! Command("special command 1")
  recoveryActor ! Command("special command 2")
}
