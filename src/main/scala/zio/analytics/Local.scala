package zio.analytics

import zio.stream.Stream

object Local {
  def evalExpr[A, B](expr: A =>: B): A => B =
    expr match {
      case _: Expression.Id[A]            => identity[A](_)
      case e: Expression.Compose[A, x, B] => evalExpr(e.f).compose(evalExpr(e.g))
      case e: Expression.FanOut[A, x, y] =>
        val ff = evalExpr(e.f)
        val gg = evalExpr(e.g)

        (in: A) => (ff(in), gg(in))
      case e: Expression.Split[x, y, w, z] =>
        val ff = evalExpr(e.f)
        val gg = evalExpr(e.g)

        (in: (x, y)) => ff(in._1) -> gg(in._2)
      case e: Expression.LongLiteral[A]    => _ => e.l
      case e: Expression.StringLiteral[A]  => _ => e.s
      case e: Expression.BooleanLiteral[A] => _ => e.b
      case Expression.Mul                  => (tp: (Long, Long)) => tp._1 * tp._2
      case Expression.Sum                  => (tp: (Long, Long)) => tp._1 + tp._2
      case Expression.Split                => (tp: (String, String)) => tp._1.split(tp._2).toList
      case e: Expression.NthColumn[A, B]   => (tp: A) => tp.asInstanceOf[Product].productElement(e.n).asInstanceOf[B]
    }

  def evalStream[A](ds: DataStream[A]): Stream[Nothing, A] =
    ds match {
      case DataStream.Literals(data) =>
        Stream.fromChunk(data).map(e => evalExpr(e)(()))

      case ds: DataStream.Map[x, A] =>
        val ff = evalExpr(ds.f)
        evalStream(ds.ds).map(ff)

      case DataStream.Filter(ds, f) =>
        val ff = evalExpr(f)
        evalStream(ds).filter(ff)

      case ds: DataStream.MapConcat[x, A] =>
        val ff = evalExpr(ds.f)
        evalStream(ds.ds).mapConcat(ff)

      case ds: DataStream.MapAccumulate[s, x, A] =>
        val zz = evalExpr(ds.z)
        val ff = evalExpr(ds.f)

        evalStream(ds.ds).mapAccum(zz(()))(Function.untupled(ff))
    }
}
