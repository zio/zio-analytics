package zio.analytics

import zio._
import zio.console.putStrLn
import zio.duration._

object WordCountSpec extends App {
  override def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] = {
    val ds = DataStream
      .fromLiterals((12L, "quick"), (15L, "quick"), (30L, "brown"), (40L, "brown"))
      .assignTimestamps(_._1)
      .groupBy(v => v.value._2)
      .foldWindow(WindowAssigner.tumbling(10.millis), 0L) { accAndEl =>
        val acc = accAndEl._1

        acc |+| 1L
      }

    println("DataStream plan:")
    pprint.pprintln(ds)

    val stream = Local.evalStream(ds)

    stream
      .foreach(tp => putStrLn(tp.toString))
      .as(0)
  }
}
