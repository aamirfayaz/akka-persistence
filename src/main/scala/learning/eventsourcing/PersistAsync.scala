package learning.eventsourcing

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence.PersistentActor

object PersistAsync extends App {


  case class Command(contents: String)
  case class Event(contents: String)

  object CriticalStreamProcessor {
    def props(eventAggregator: ActorRef) = Props(new CriticalStreamProcessor(eventAggregator))
  }

  class CriticalStreamProcessor(eventAggregator: ActorRef) extends PersistentActor with ActorLogging {

    override def persistenceId: String = "critical-stream-processor"

    override def receiveCommand: Receive = {
      case Command(contents) =>
        eventAggregator ! s"Processing $contents"
        // mutate
        persistAsync(Event(contents)) /*                          TIME GAP                            */ { e =>
          eventAggregator ! e
          // mutate
        }

        // some actual computation
        val processedContents = contents + "_processed"
        persistAsync(Event(processedContents)) /*                          TIME GAP                            */ { e =>
          eventAggregator ! e
        }
    }

    override def receiveRecover: Receive = {
      case message => log.info(s"Recovered: $message")
    }
  }

  class EventAggregator extends Actor with ActorLogging {
    override def receive: Receive = {
      case message => log.info(s"$message")
    }
  }


  val system = ActorSystem("PersistAsyncDemo")
  val eventAggregator = system.actorOf(Props[EventAggregator], "eventAggregator")
  val streamProcessor = system.actorOf(CriticalStreamProcessor.props(eventAggregator), "streamProcessor")

  streamProcessor ! Command("command1")
  streamProcessor ! Command("command2")

  /*
    persistAsync vs persist
    - perf: high-throughput environments when event ordering is not an issue
    - bad because state will get modified non-deterministically, so inconsistent state
    - we mostly save commands send to receiveCommand directly in persistAsync without creating an event separately with id
      i.e mix persistAsync with command-sourcing

    persist vs persisAsync
    - ordering guarantees

    This always be the trade-off that we have to make between persist and persistAsync, in  most of cases we will use persist
    But in high throughput environments where it is critical that you process events as fast as you can
    without needing to somehow maintain their order persistAsync will be our choice.
   */

}
