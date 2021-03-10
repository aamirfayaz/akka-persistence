import scala.collection.mutable
import scala.util.Random

var q = new mutable.Queue[Int]()
q.enqueue(1,2,3,4,5)
q.toString()
q.dequeue()
q.toString()
q.enqueue(22)
q.toString()

val r = new Random()
r.nextInt(5)
r.nextInt(5)
r.nextInt(5)
r.nextInt(5)
r.nextInt(5)
r.nextInt(5)
r.nextInt(5)
r.nextInt(5)
r.nextInt(5)

