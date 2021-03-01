package learning

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.{PersistentActor, SaveSnapshotFailure, SaveSnapshotSuccess, SnapshotOffer}

import scala.collection.mutable

/**
  * Problem: Long lived entities take a lot of time to recover
  * i.e long lived persistent actors may take lot of time to recover if they save lot and lots of events
  * Solution: Save Checkpoints i.e  to save checkpoints along the way known as Snapshots
  * i.e save the entire state as check-points (snapshots)
  * recover the latest snap shot + events since then
  * we can also customize which snapshot to recover from other than the latest, but rare
  *

  * e.g whatz-app type messenger, to persist messages, take long to recover all messages
  *
  * Saving Snapshots: dedicated store, async call.
  * snapshot save can fail, but not big deal, remember snapshots are optional,
  * if we miss a save snapshot or snapshot store crashes, that's ok, we can still do full recovery with events from journal, so
  * snapshot-store doesn't need to as robust and as fault tolerant as journal.
  */
object SnapShots1 extends App {

  //commands
  case class ReceivedMessage(contents: String) // message from your contact
  case class SendMessage(contents: String) // message to your contact

  //events
  case class ReceivedMessageRecord(id: Int, contents: String)

  case class SentMessageRecord(id: Int, contents: String)

  object Chat {
    def props(owner: String, contact: String): Props = Props(new Chat(owner, contact))
  }

  class Chat(owner: String, contact: String) extends PersistentActor with ActorLogging {

    var currentMessageId = 0
    var commandsWithoutCheckpoint = 0
    val MAX_MESSAGES = 10
    val lastMessages = new mutable.Queue[(String, String)]()


    override def receiveRecover: Receive = {
      case ReceivedMessageRecord(id, contents) =>
        log.info(s"Recovered Received Message, id: $id, contents: $contents")
        maybeReplaceMessage(contact, contents)
        currentMessageId = id
      case SentMessageRecord(id, contents) =>
        log.info(s"Recovered Sent Message, id: $id, contents: $contents")
        maybeReplaceMessage(owner, contents)
        currentMessageId = id
      case SnapshotOffer(metadata, contents) => //give latest snapshot saved
        log.info(s"Recovered snapshot, metadata: $metadata")
        contents.asInstanceOf[mutable.Queue[(String, String)]].foreach(lastMessages.enqueue(_))
    }

    override def receiveCommand: Receive = {
      case ReceivedMessage(contents) =>
        persist(ReceivedMessageRecord(currentMessageId, contents)) { e =>
          log.info(s"Received Message: $contents")
          maybeReplaceMessage(contact, contents)
          currentMessageId += 1
          maybeCheckpoint()
        }
      case SendMessage(contents) =>
        persist(SentMessageRecord(currentMessageId, contents)) { e =>
          log.info(s"Send Message: $contents")
          maybeReplaceMessage(owner, contents)
          currentMessageId += 1
          maybeCheckpoint
        }
      case "print" =>
        log.info(s"Most recent messages: $lastMessages")
      case SaveSnapshotSuccess(metadata) => log.info(s"Saving snapshot succeeded: $metadata")
      case SaveSnapshotFailure(metadata, causeForFailure) => log.warning(s"Saving snapshot: $metadata, failed because of: $causeForFailure")
    }

    def maybeReplaceMessage(sender: String, contents: String) = {
      if (lastMessages.size >= MAX_MESSAGES) {
        lastMessages.dequeue()
      }
      lastMessages.enqueue((sender, contents))
    }

    def maybeCheckpoint() = {
      commandsWithoutCheckpoint += 1
      if(commandsWithoutCheckpoint >= MAX_MESSAGES) {
        log.info("Saving checkpoint...")
        saveSnapshot(lastMessages) //async
        commandsWithoutCheckpoint = 0
      }
    }

    override def persistenceId: String = s"$owner-$contact-chat"
  }

  val system = ActorSystem("SnapShotDemo1")
  val chat = system.actorOf(Chat.props("aamir", "daniel"))

/*  for(i <- 1 to 100000) {
    chat ! ReceivedMessage(s"akka rocks: $i")
    chat ! SendMessage(s"akka rules: $i")
  }*/

  chat ! "print"
  /**
    * As lot of recover events, so snapshots to rescue,
    * for every 10 messages, we are going to persist entire state, queue here,
    * directly in a dedicated persistent store known as snapshot store
    */

  /*
  event 1
  event 2
  event 3
  snapshot 1
  event 4
  snapshot 2
  event 5
  event 6

  //recovery = latest snapshot, and all events since that snapshot
  //here in above commented example: on recovery, snapshot 2 + event 5 and event 6 will be recovered, so in our case
  latest snapshot plus 10 more events were recovered
 */

  /*
  pattern:
  - after each persist, maybe save a snapshot (logic is up to you)
  - if you save a snapshot, handle the SnapshotOffer message in receiveRecover
  - (optional, but best practice) handle SaveSnapshotSuccess and SaveSnapshotFailure in receiveCommand
  - profit from the extra speed!!!
 */

}