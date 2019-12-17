package zio.analytics

object WordCountSpec {
  val ds = DataStream
    .fromLiterals("a quick", "brown fox")
    .mapConcat(_.split(" "))
    .mapAccumulate(0L) { tp =>
      (tp._1 |+| 1L, tp._2)
    }
}
