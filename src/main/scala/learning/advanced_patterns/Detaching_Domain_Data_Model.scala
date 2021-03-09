package learning.advanced_patterns

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.journal.{EventAdapter, EventSeq}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

/**
  * - Domain model = events our actor thinks it persists.
  * - Data model = objects which actually get persisted.
  * - good practice: make the two models independent.
  * - awesome side effect: easier management of schema evolution.
  *
  * Benefit:
     - persistence is transparent to the actor [business model and persistence actor unaware of any schema evolution].
     - schema evolution is done in the adapters only.
  */

object DomainModel {

  case class User(id: String, email: String, name: String)

  case class Coupon(code: String, promotionAmount: Int)

  //command
  case class ApplyCoupon(coupon: Coupon, user: User)

  //event
  case class CouponApplied(code: String, user: User)

}

object DataModel {

  //definition of object that is actually end up in journal
  case class WrittenCouponApplied(code: String, userId: String, userEmail: String)
  case class WrittenCouponAppliedV2(code: String, userId: String, userEmail: String, name: String)

}

/**
  * This Adapter will converter Domain Model Events to Data Model events
  */
class ModelAdapter extends EventAdapter {

  import DataModel._
  import DomainModel._

  //journal -> serializer -> fromJournal -> actor
  override def fromJournal(event: Any, manifest: String): EventSeq = event match {
    case event@WrittenCouponApplied(code, userId, userEmail) =>
      println(s"Converting DATA model $event to DOMAIN model")
      EventSeq.single(CouponApplied(code, User(userId, userEmail, "")))
    case event@WrittenCouponAppliedV2(code, userId, userEmail, userName) =>
      println(s"Converting DATA model $event to DOMAIN model")
      EventSeq.single(CouponApplied(code, User(userId, userEmail, userName)))
    case other => EventSeq.single(other)
  }

  override def manifest(event: Any): String = "CMA" //don't really need this.

  //actor -> toJournal -> serializer -> journal
  override def toJournal(event: Any): Any = event match {
    case event@CouponApplied(code, user) =>
      println(s"Converting DOMAIN $event to DATA model")
      WrittenCouponAppliedV2(code, user.id, user.email, user.name)

  }
}

object Detaching_Domain_Data_Model extends App {

  /**
    * So, this actor was untouched, as if nothing had happened, that is take-away from this exercise
    * so schema versioning happens only in data model, i.e V2, and event adapter (1 more case added and some changes small).
    */
  class CouponManager extends PersistentActor with ActorLogging {

    import DomainModel._

    val coupons: mutable.Map[String, User] = new mutable.HashMap[String, User]()

    override def receiveRecover: Receive = {
      case event@CouponApplied(code, user) =>
        log.info(s"Recovered $event")
        coupons.put(code, user)
    }

    override def receiveCommand: Receive = {
      case ApplyCoupon(coupon, user) =>
        if (!coupons.contains(coupon.code)) {
          persist(CouponApplied(coupon.code, user)) { e =>
            log.info(s"Persisted ${e}..")
            coupons.put(coupon.code, user)
          }
        }
    }

    override def persistenceId: String = "coupon-manager"
  }

  val system = ActorSystem("DetachingModels", ConfigFactory.load().getConfig("detachingModels"))
  val couponManager = system.actorOf(Props[CouponManager], "CouponManager")

  import DomainModel._

/*  for (i <- 10 to 15) {
    val coupon = Coupon(s"MEGA_COUPON_$i", 100)
    val user = User(s"$i", s"user_$i@learningpersistence.com", "John Doe")
    couponManager ! ApplyCoupon(coupon, user)
  }*/


}