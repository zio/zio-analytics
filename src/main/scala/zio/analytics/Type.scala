package zio.analytics

sealed abstract class Type[A] {
  def lift[B](a: A): Expression[B, A]
}

object Type {
  case object Unknown extends Type[Any] {
    def lift[B](a: Any): Expression[B, Any] = ???
  }

  case object Boolean extends Type[Boolean] {
    def lift[B](b: Boolean): Expression[B, Boolean] = Expression.BooleanLiteral(b)
  }

  case object String extends Type[String] {
    def lift[B](s: String): Expression[B, String] = Expression.StringLiteral(s)
  }

  case object Long extends Type[Long] {
    def lift[B](a: Long): Expression[B, Long] = Expression.LongLiteral(a)
  }

  case class Tuple2[A, B](fst: Type[A], snd: Type[B]) extends Type[(A, B)] {
    def lift[C](a: (A, B)): Expression[C, (A, B)] =
      Expression.FanOut(fst.lift(a._1), snd.lift(a._2))
  }

  implicit val longType: Type[Long]       = Long
  implicit val stringType: Type[String]   = String
  implicit val booleanType: Type[Boolean] = Boolean
  implicit val unknownType: Type[Any]     = Unknown
  implicit def tuple2Type[A, B](implicit A: Type[A], B: Type[B]): Type[(A, B)] =
    Tuple2(A, B)
}
