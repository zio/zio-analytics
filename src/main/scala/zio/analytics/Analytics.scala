package zio.analytics

trait Analytics {

  /**
   * A distributed, streaming computation.
   */
  type DataStream[_]

  /**
   * An abstract function evaluated in a computation.
   */
  type =>:[-A, +B]

  /**
   * A type that is representable in a streaming computation.
   */
  type Type[_]

  /**
   *
   */
  type Grouped[K, A]

  /**
   * A type with no schema information.
   */
  type Unknown

  /**
   * Proofs that the primitives can be represented in this module.
   */
  implicit val longType: Type[Long]
  implicit val stringType: Type[String]
  implicit val booleanType: Type[Boolean]
  implicit val unknownType: Type[Unknown]
  implicit def tuple2Type[A, B](implicit A: Type[A], B: Type[B]): Type[(A, B)]

  /**
   * Higher-order composition functions.
   */
  trait Functions {
    def id[A]: A =>: A
    def andThen[A, B, C](f: A =>: B, g: B =>: C): A =>: C
    def compose[A, B, C](f: B =>: C, g: A =>: B): A =>: C
    def fanOut[A, B, C](f: A =>: B, g: A =>: C): A =>: (B, C)
    def split[A, B, C, D](f: A =>: B, g: C =>: D): (A, C) =>: (B, D)
  }
  val functions: Functions

  implicit class FunctionOps[A, B](f: A =>: B) {
    def >>>[C](g: B =>: C): A =>: C              = functions.andThen(f, g)
    def <<<[C](g: C =>: A): C =>: B              = functions.compose(f, g)
    def &&&[C](g: A =>: C): A =>: (B, C)         = functions.fanOut(f, g)
    def ***[C, D](g: C =>: D): (A, C) =>: (B, D) = functions.split(f, g)
  }

  /**
   * Literal lifting operations.
   */
  implicit def liftLong[A](l: Long): A =>: Long
  implicit def liftString[A](s: String): A =>: String
  implicit def liftBoolean[A](b: Boolean): A =>: Boolean
  implicit def liftTuple[A, B, C](tp: (A =>: B, A =>: C)): A =>: (B, C)

  trait Numbers {
    def mul: (Long, Long) =>: Long
    def sum: (Long, Long) =>: Long
  }
  val numbers: Numbers

  implicit class NumberOps[A](x: A =>: Long) {
    def |+|(y: A =>: Long): A =>: Long = (x &&& y) >>> numbers.sum
    def *(y: A =>: Long): A =>: Long   = (x &&& y) >>> numbers.mul
  }

  trait Strings {
    def split: (String, String) =>: List[String]
  }
  val strings: Strings

  implicit class StringOps[A](s: A =>: String) {
    def split(delimiter: A =>: String): A =>: List[String] = (s &&& delimiter) >>> strings.split
  }

  trait Tuples {
    def first[A, B]: (A, B) =>: A
    def second[A, B]: (A, B) =>: B
  }

  val tuples: Tuples

  implicit class TupleOps[A, B, C](tp: A =>: (B, C)) {
    def _1: A =>: B = tp >>> tuples.first
    def _2: A =>: C = tp >>> tuples.second
  }

  trait DataStreamFunctions {
    def fromLiterals[A: Type](data: (A =>: A)*): DataStream[A]
    def map[A, B: Type](ds: DataStream[A])(f: (A =>: A) => (A =>: B)): DataStream[B]
    def filter[A](ds: DataStream[A])(f: (A =>: A) => (A =>: Boolean)): DataStream[A]
    def mapConcat[A, B: Type](ds: DataStream[A])(f: (A =>: A) => (A =>: List[B])): DataStream[B]
    def mapAccumulate[S: Type, A, B: Type](ds: DataStream[A])(z: (A =>: S))(
      f: (A =>: (S, A)) => (A =>: (S, B))
    ): DataStream[(S, B)]
  }
  val DataStream: DataStreamFunctions

  implicit class DataStreamOps[A](ds: DataStream[A]) {
    def map[B: Type](f: (A =>: A) => (A =>: B)): DataStream[B]             = DataStream.map(ds)(f)
    def mapConcat[B: Type](f: (A =>: A) => (A =>: List[B])): DataStream[B] = DataStream.mapConcat(ds)(f)
    def filter(ds: DataStream[A])(f: (A =>: A) => (A =>: Boolean)): DataStream[A] =
      DataStream.filter(ds)(f)
    def mapAccumulate[S: Type, B: Type](z: (A =>: S))(f: (A =>: (S, A)) => (A =>: (S, B))): DataStream[(S, B)] =
      DataStream.mapAccumulate(ds)(z)(f)
  }
}
