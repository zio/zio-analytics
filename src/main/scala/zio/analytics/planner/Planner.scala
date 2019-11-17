package zio.analytics.planner

import zio.analytics.Analytics

trait Planner extends Analytics {
  import zio.Chunk

  type DataStream[A] = DataStreamPlan
  type =>:[-A, +B]   = Expression
  type Type[A]       = Reified
  type Unknown       = Reified.Unknown.type

  val functions: Functions = new Functions {
    def andThen[A, B, C](f: A =>: B, g: B =>: C): A =>: C            = Expression.Compose(g, f)
    def compose[A, B, C](f: B =>: C, g: A =>: B): A =>: C            = Expression.Compose(f, g)
    def fanOut[A, B, C](f: A =>: B, g: A =>: C): A =>: (B, C)        = Expression.FanOut(f, g)
    def id[A]: A =>: A                                               = Expression.Id
    def split[A, B, C, D](f: A =>: B, g: C =>: D): (A, C) =>: (B, D) = Expression.Split(f, g)
  }

  val DataStream: DataStreamFunctions = new DataStreamFunctions {
    def map[A, B: Type](ds: DataStream[A])(f: (A =>: A) => (A =>: B)): DataStream[B] =
      DataStreamPlan.Map(ds, f(functions.id))

    def fromLiterals[A: Type](data: (A =>: A)*): DataStream[A] =
      DataStreamPlan.Literals(Chunk.fromIterable(data))

    def filter[A](ds: DataStream[A])(f: (A =>: A) => (A =>: Boolean)): DataStream[A] =
      DataStreamPlan.Filter(ds, f(functions.id))

    def mapConcat[A, B: Type](ds: DataStream[A])(f: (A =>: A) => (A =>: List[B])): DataStream[B] =
      DataStreamPlan.MapConcat(ds, f(functions.id))

    def mapAccumulate[S: Type, A, B: Type](ds: DataStream[A])(z: A =>: S)(f: (A =>: (S, A)) => (A =>: (S, B))) =
      DataStreamPlan.MapAccumulate(ds, z, f(z &&& functions.id))
  }

  implicit def liftLong[A](l: Long): A =>: Long          = Expression.LongLiteral(l)
  implicit def liftString[A](s: String): A =>: String    = Expression.StringLiteral(s)
  implicit def liftBoolean[A](b: Boolean): A =>: Boolean = Expression.BooleanLiteral(b)
  implicit def liftTuple[A, B, C](tp: (A =>: B, A =>: C)): A =>: (B, C) =
    functions.fanOut(tp._1, tp._2)

  val numbers: Numbers = new Numbers {
    def mul: (Long, Long) =>: Long = Expression.Mul
    def sum: (Long, Long) =>: Long = Expression.Sum
  }

  val strings: Strings = new Strings {
    def split: (String, String) =>: List[String] = Expression.Split
  }

  implicit override val booleanType: Type[Boolean] = Reified.Boolean
  implicit override val longType: Type[Long]       = Reified.Long
  implicit override val stringType: Type[String]   = Reified.String
  implicit override val unknownType: Type[Unknown] = Reified.Unknown
  implicit override def tuple2Type[A, B](implicit A: Type[A], B: Type[B]): Type[(A, B)] =
    Reified.Tuple2(A, B)

  val tuples: Tuples = new Tuples {
    def first[A, B]: (A, B) =>: A  = Expression.NthColumn(0)
    def second[A, B]: (A, B) =>: B = Expression.NthColumn(1)
  }
}
