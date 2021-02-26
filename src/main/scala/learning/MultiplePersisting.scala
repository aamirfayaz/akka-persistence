package learning

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence.PersistentActor

import java.util.Date

/**
  * No, not persistAll,
  * we want to persist many different events to the journal in the handler of one command
  * We are interested in delivery, persisting and handling guarantees
  *
  */

object MultiplePersisting1 extends App {
  /**
    * Diligent accountant: with every invoice, will persist TWO events:
    * 1. a tax record for the fiscal authority
    * 2. an invoice record for personal logs or some auditing authority
    */
  case class Invoice(recipient: String, date: Date, amount: Int)
  case class TaxRecord(taxId: String /**taxId: for company this tax record is for*/,
                       recordId: Int, date: Date, totalAmount: Int
                      )
  case class InvoiceRecord(invoiceRecordId: Int, recipient: String, date: Date, amount: Int)

  object DiligentAccountant {
    def props(taxId: String, taxAuthority: ActorRef):Props = Props(new DiligentAccountant(taxId, taxAuthority))
  }

  class DiligentAccountant(taxId: String, taxAuthority: ActorRef) extends PersistentActor with ActorLogging {

    var nextTaxRecordId = 0
    var nextInvoiceRecordId = 0
    override def receiveRecover: Receive = {
      case event => log.info(s"Recovered: $event")
    }

    override def receiveCommand: Receive = {
      case Invoice(recipient, date, amount) =>

        /**
          * If sender and destination are the same, message send in order,
          * so because tax-record is persisted before invoice-record, they are same like ! calls,
          * so its guaranteed that tax-record call to taxAuthority will go first then invoice-record.
          * So even if calls to persist is async as we know but for sure ordering of events is guaranteed.
          * since call to journal also ! aync, called in order so execution of handlers also occur in order
          *
          * General-rule: Sequence calls to persist will happen in order
          *
          * For declarations also, same logic will follow and we will receive messages in order
          */
        //like journal ! TaxRecord PERSISTENCE is also based on message passing i.e journal are actually implemented using actors
        persist(TaxRecord(taxId, nextTaxRecordId, date, amount / 3)) { taxEvent =>
           taxAuthority ! taxEvent
           nextTaxRecordId += 1
           persist(s"I here by declare this tax record for $recipient to be true and complete") { declaration =>
             taxAuthority ! declaration
           }
        }

        //actually like journal ! TaxRecord --async
        persist(InvoiceRecord(nextInvoiceRecordId, recipient, date, amount)) { invoiceEvent =>
          taxAuthority ! invoiceEvent
          nextInvoiceRecordId += 1
          persist(s"I here by declare this invoice record for $recipient to be true and complete") { declaration =>
            taxAuthority ! declaration
          }
        }
    }

    override def persistenceId: String = "diligent-accountant"
  }

  class TaxAuthority extends Actor with ActorLogging {
    override def receive: Receive = {
      case message => log.info(s"Received: $message")
    }
  }
  val system = ActorSystem("MultiplePersistsDemo")
  val taxAuthority = system.actorOf(Props[TaxAuthority], "fiscalAuthority")
  val accountant = system.actorOf(DiligentAccountant.props("UK_Some_Company", taxAuthority))

  accountant ! Invoice("Shehzal_Corp", new Date, 2000)
  accountant ! Invoice("Shehzal_Software", new Date, 6000) // its stashed till first is not completed

  /**
    * The message ordering to taxAuthority i.e first TaxRecord and then InvoiceRecord is guaranteed
    * PERSISTENCE is also based on message passing i.e journal are actually implemented using actors
    */

  /**
    * Nested persisting
    * e.g do a declaration after I paid tax or stored invoice
    * still order guaranteed in that
    */

  /**
    * So,
    * Calls to persist are executed in order, also,
    * Handler for subsequent persist() calls are executed in order
    * A nested persist is called after the enclosing persist
    */

}
