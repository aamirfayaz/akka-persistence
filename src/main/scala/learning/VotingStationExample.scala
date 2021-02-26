package learning

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

import scala.collection.mutable

/**
  * Persistent Actor for a voting station
  * keep:
  * the citizens who voted
  * the poll: mapping b/w a candidate and the number of received votes
  * The actor must be able to recover it's state if shutdown/restarted
  */
object VotingStationApp extends App {

  case class Vote(aadharCardNumber: String, candidate: String)

  class VotingStation extends PersistentActor with ActorLogging {

    val citizens: mutable.Set[String] = new mutable.HashSet[String]()
    val poll: mutable.Map[String, Int] = new mutable.HashMap[String, Int]()

    override def receiveCommand: Receive = {
      case vote@Vote(aadharCardNumber, candidate) =>
        if (!citizens.contains(aadharCardNumber)) {
          persist(vote) { _ =>
            log.info(s"Persisted: $vote")
            handleInternalStateChange(aadharCardNumber, candidate)
          }
        } else {
          log.warning(s"Citizen $aadharCardNumber is trying to vote multiple times!")
        }
      case "print" =>
        log.info(s"Current state: \nCitizens: $citizens\npolls: $poll")
    }

    def handleInternalStateChange(aadharCardNumber: String, candidate: String): Unit = {
      citizens.add(aadharCardNumber)
      val votes = poll.getOrElse(candidate, 0)
      poll.put(candidate, votes + 1)
    }


    override def receiveRecover: Receive = {
      case vote@Vote(aadharCardNumber, candidate) =>
        log.info(s"Recovered: $vote")
        handleInternalStateChange(aadharCardNumber, candidate)
    }

    override def persistenceId: String = "simple-voting-system"
  }

  val system = ActorSystem("PersistentActorsExercise")
  val votingStation = system.actorOf(Props[VotingStation], "simpleVotingStation")


  val votesMap = Map[String, String](
    "ALice: aadharnumber1" -> "Martin",
    "Bob: aadharnumber2" -> "Roland",
    "Charlie: aadharnumber3" -> "Martin",
    "David: aadharnumber4" -> "Jonas",
    "Daniel: aadharnumber5" -> "Martin"
  )

/*  votesMap.keys.foreach { citizen =>
    votingStation ! Vote(citizen, votesMap(citizen))
  }
*/
  votesMap.foreach { case (citizen, candidate) =>
    votingStation ! Vote(citizen, candidate)
  }

  votingStation ! "print"
  //processed after receiveRecover
  /*votingStation ! Vote("David: aadharnumber4", "Mary") // illegal
  votingStation ! "print"*/
}