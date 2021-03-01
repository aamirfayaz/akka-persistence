import scala.collection.mutable

var q = new mutable.Queue[Int]()
q.enqueue(1,2,3,4,5)
q.toString()
q.dequeue()
q.toString()
q.enqueue(22)
q.toString()
