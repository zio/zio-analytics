package zio.analytics

import zio._
import zio.console.putStrLn

object WordCountSpec extends App {
  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] = {
    val ds = DataStream
      .fromLiterals("quick", "quick", "brown", "brown")
      .mapConcat(_.split(" "))
      .groupBy(word => (word, 1L))
      .fold(group => (group.key, group.values.length))

    val stream = Local.evalStream(ds)

    stream
      .foreach(tp => putStrLn(tp.toString))
      .as(0)
  }
}
