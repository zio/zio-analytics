package zio.analytics

sealed abstract class Type[A] {
  def lift(a: A): Expression[Unit, A]
}

object Type {
  case object Unknown extends Type[Any] {
    def lift(a: Any): Expression[Unit, Any] = ???
  }

  case object Boolean extends Type[Boolean] {
    def lift(b: Boolean): Expression[Unit, Boolean] = Expression.BooleanLiteral(b)
  }

  case object String extends Type[String] {
    def lift(s: String): Expression[Unit, String] = Expression.StringLiteral(s)
  }

  case object Long extends Type[Long] {
    def lift(a: Long): Expression[Unit, Long] = Expression.LongLiteral(a)
  }

  case class Tuple2[A, B](fst: Type[A], snd: Type[B]) extends Type[(A, B)] {
    def lift(a: (A, B)): Expression[Unit, (A, B)] =
      Expression.FanOut(fst.lift(a._1), snd.lift(a._2))
  }

  implicit val longType: Type[Long]       = Long
  implicit val stringType: Type[String]   = String
  implicit val booleanType: Type[Boolean] = Boolean
  implicit val unknownType: Type[Any]     = Unknown
  implicit def tuple2Type[A, B](implicit A: Type[A], B: Type[B]): Type[(A, B)] =
    Tuple2(A, B)
}
