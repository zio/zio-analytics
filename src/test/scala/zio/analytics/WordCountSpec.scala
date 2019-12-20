package zio.analytics

import zio._
import zio.console.putStrLn

object WordCountSpec extends App {
  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] = {
    val ds = DataStream
      .fromLiterals("quick", "quick", "brown", "brown")
      .mapConcat(_.split(" "))
      .groupBy(word => (word, 1L))
      .mapValues(_ * 4L)
      .fold(group => (group.key, group.values.sum))
      .mapAccumulate(0L) { tp =>
        val state    = tp._1
        val element  = tp._2
        val newState = state |+| element._2

        (newState, Expression.sequenceTuple((newState, tp._2)))
      }

    println("DataStream plan:")
    pprint.pprintln(ds)

    val stream = Local.evalStream(ds)

    stream
      .foreach(tp => putStrLn(tp.toString))
      .as(0)
  }
}
