package zio.analytics

import zio._
import zio.stream._

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
      case e: Expression.KeyValue[A, k, v] =>
        val keyExpr   = evalExpr(e.key)
        val valueExpr = evalExpr(e.value)

        (a: A) => Grouped(keyExpr(a), valueExpr(a))
      case _: Expression.Length[v] =>
        (l: List[v]) => l.length.toLong
      case _: Expression.GroupKey[k, v] =>
        (g: Group[k, v]) => g.key
      case _: Expression.GroupValues[k, v] =>
        (g: Group[k, v]) => g.values.toSeq.toList
      case Expression.ListSum =>
        (l: List[Long]) => l.sum
      case _: Expression.TimestampedValue[a] =>
        (a: Timestamped[a]) => a.value
      case _: Expression.TimestampedTimestamp[a] =>
        (a: Timestamped[a]) => a.timestamp
    }

  sealed trait Record[+A] extends Product with Serializable { self =>
    def map[B](f: A => B): Record[B] =
      self match {
        case Element(a)   => Element(f(a))
        case Watermark(m) => Watermark(m)
      }
  }

  case class Element[+A](a: A)     extends Record[A]
  case class Watermark(mark: Long) extends Record[Nothing]

  implicit class ZStreamOps[R, E, A](self: ZStream[R, E, A]) {
    import ZStream.GroupBy

    /**
     * More powerful version of [[ZStream.groupByKey]]
     */
    final def groupByOrBroadcast[R1 <: R, E1 >: E, K, V](
      f: A => ZIO[R1, E1, (Option[K], V)],
      buffer: Int = 16
    ): ZStream.GroupBy[R1, E1, K, V] = {
      val qstream = ZStream.unwrapManaged {
        for {
          decider <- Promise.make[Nothing, (Option[K], V) => UIO[Int => Boolean]].toManaged_
          out <- Queue
                  .bounded[Take[E1, (K, GroupBy.DequeueOnly[Take[E1, V]])]](buffer)
                  .toManaged(_.shutdown)
          emit <- Ref.make[Boolean](true).toManaged_
          ref  <- Ref.make[Map[K, Int]](Map()).toManaged_
          add <- self
                  .mapM(f)
                  .distributedDynamicWith(
                    buffer,
                    (kv: (Option[K], V)) => decider.await.flatMap(_.tupled(kv)),
                    out.offer
                  )
          _ <- decider.succeed {
                case (None, _) => ZIO.succeed(_ => true)
                case (Some(k), _) =>
                  ref.get.map(_.get(k)).flatMap {
                    case Some(idx) => ZIO.succeed(_ == idx)
                    case None =>
                      emit.get.flatMap {
                        case true =>
                          add.flatMap {
                            case (idx, q) =>
                              (ref.update(_ + (k -> idx)) *>
                                out.offer(Take.Value(k -> q.map(_.map(_._2))))).as(_ == idx)
                          }
                        case false => ZIO.succeed(_ => false)
                      }
                  }
              }.toManaged_
        } yield ZStream.fromQueueWithShutdown(out).unTake
      }
      new ZStream.GroupBy(qstream, buffer)
    }

  }

  def evalStream[A](ds: DataStream[A]): Stream[Nothing, Record[A]] =
    ds match {
      case DataStream.Literals(data) =>
        Stream.fromChunk(data).map(e => Element(evalExpr(e)(()))) ++ Stream(Watermark(Long.MaxValue))

      case ds: DataStream.Map[x, A] =>
        val ff = evalExpr(ds.f)
        evalStream(ds.ds).map(_.map(ff))

      case DataStream.Filter(ds, f) =>
        val ff = evalExpr(f)

        evalStream(ds).filter {
          case Element(a)   => ff(a)
          case Watermark(_) => true
        }

      case ds: DataStream.MapConcat[x, A] =>
        val ff = evalExpr(ds.f)

        evalStream(ds.ds).mapConcat {
          case w @ Watermark(_) => List(w)
          case Element(a)       => ff(a).map(Element(_))
        }

      case ds: DataStream.MapAccumulate[s, x, A] =>
        val zz = evalExpr(ds.z)
        val ff = evalExpr(ds.f)

        evalStream(ds.ds)
          .mapAccum(zz(())) { (s, rec) =>
            rec match {
              case w @ Watermark(_) => (s, w)
              case Element(x) =>
                val (s2, a) = ff(s -> x)
                (s2, Element(a))
            }
          }

      case ds: DataStream.GroupBy[a, k] =>
        val extractKey = evalExpr(ds.f)

        evalStream(ds.ds).map {
          case w @ Watermark(_) => w
          case Element(a)       => Element(Grouped(extractKey(a), a))
        }

      case ds: DataStream.Fold[k, v, A] =>
        val reducer = evalExpr(ds.f)

        evalStream(ds.ds).groupByOrBroadcast {
          case w @ Watermark(_) => UIO.succeed(None          -> w)
          case Element(rec)     => UIO.succeed(Some(rec.key) -> Element(rec.value))
        } { (k, vs) =>
          Stream.fromEffect(
            vs.collect {
              case Element(v) => v
            }.runCollect
              .map(vs => Element(reducer(Group(k, Chunk.fromIterable(vs)))))
          )
        }

      case ds: DataStream.MapValues[k, v, b] =>
        val ff = evalExpr(ds.f)

        evalStream(ds.ds).map {
          case w @ Watermark(_)       => w
          case Element(Grouped(k, v)) => Element(Grouped(k, ff(v)))
        }

      case ds: DataStream.AssignTimestamps[a] =>
        val ff = evalExpr(ds.f)

        evalStream(ds.ds).collect {
          // Timestamp reassignment invalidates watermarks, so we only let the
          // final flushing watermark through.
          case w @ Watermark(Long.MaxValue) => w
          case e @ Element(_)               => e
        }.map(_.map(a => Timestamped(ff(a), a)))

      case ds: DataStream.FoldWindow[k, v, s] =>
        val z      = evalExpr(ds.z)
        val f      = evalExpr(ds.f)
        val stream = evalStream(ds.ds)

        stream.mapConcat {
          case w @ Watermark(_) => List(w)
          case Element(g) =>
            val key     = g.key
            val windows = ds.window.assign(g.value.timestamp)

            windows.map(w => Element(Grouped(w -> key, g.value.value)))
        }.groupByOrBroadcast {
          case w @ Watermark(_) =>
            UIO.succeed(None -> w)
          case Element(g) =>
            UIO.succeed(Some(g.key) -> Element(g.value))
        } {
          case ((window, k), vs) =>
            vs.mapAccum(z(()) -> -1L) {
                case ((s, currMark), Element(v)) =>
                  if (currMark <= window.upper)
                    ((f((s, window, v)), currMark), List())
                  else
                    ((s, currMark), List())

                case ((s, currMark), Watermark(w)) =>
                  if (w > currMark && w > window.upper)
                    ((z(()), w), List(Watermark(w), Element(Grouped(k, Windowed(window, s)))))
                  else
                    ((s, w), List())
              }
              .mapConcat(identity)
        }
    }
}
