package zio.analytics

import zio.Chunk

sealed abstract class DataStream[A]
object DataStream {
  trait ComputeGrouping[A, K] {
    type Out
    def compute: (K, A) =>: Out
  }

  object ComputeGrouping extends LowPrioComputeGrouping {
    type Aux[A, K, Out0] = ComputeGrouping[A, K] { type Out = Out0 }
    implicit def alreadyGrouped[A, K0, K]: Aux[Grouped[K0, A], K, Grouped[(K0, K), A]] =
      new ComputeGrouping[Grouped[K0, A], K] {
        type Out = Grouped[(K0, K), A]
        def compute: (K, Grouped[K0, A]) =>: Out = {
          val k0: Grouped[K0, A] =>: K0 = Expression.GroupedKey()
          val idk: K =>: K              = Expression.Id[K]()

          val keytp: (K, Grouped[K0, A]) =>: (K0, K) = Expression.Split(idk, k0) >>> Expression.FlipTuple()

          val a: (K, Grouped[K0, A]) =>: A =
            Expression.NthColumn[(K, Grouped[K0, A]), Grouped[K0, A]](1) >>>
              Expression.GroupedValue[K0, A]

          (keytp &&& a) >>> Expression.ConstructGrouped()
        }
      }
  }

  trait LowPrioComputeGrouping {
    implicit def otherwise[A, K]: ComputeGrouping.Aux[A, K, Grouped[K, A]] =
      new ComputeGrouping[A, K] {
        type Out = Grouped[K, A]
        def compute: (K, A) =>: Grouped[K, A] = Expression.ConstructGrouped[K, A]()
      }
  }

  case class Literals[A](data: Chunk[Expression[Unit, A]])                 extends DataStream[A]
  case class Map[A, B](ds: DataStream[A], f: Expression[A, B])             extends DataStream[B]
  case class MapConcat[A, B](ds: DataStream[A], f: Expression[A, List[B]]) extends DataStream[B]
  case class Filter[A](ds: DataStream[A], f: Expression[A, Boolean])       extends DataStream[A]
  case class MapAccumulate[S, A, B](ds: DataStream[A], z: Expression[Unit, S], f: Expression[(S, A), (S, B)])
      extends DataStream[B]
  case class GroupBy[A, K, R](ds: DataStream[A], f: Expression[A, K], c: ComputeGrouping.Aux[A, K, R])
      extends DataStream[R]
  case class Fold[K, V, R](ds: DataStream[Grouped[K, V]], f: Expression[Group[K, V], R]) extends DataStream[R]
  case class MapValues[K, V, B](ds: DataStream[Grouped[K, V]], f: Expression[V, B])      extends DataStream[Grouped[K, B]]
  case class AssignTimestamps[A](ds: DataStream[A], f: Expression[A, Long])              extends DataStream[Timestamped[A]]
  case class FoldWindow[K, V, S](
    ds: DataStream[Grouped[K, Timestamped[V]]],
    window: WindowAssigner,
    z: Expression[Unit, S],
    f: Expression[(S, Window, V), S]
  ) extends DataStream[Grouped[K, Windowed[S]]]

  implicit class Ops[A](ds: DataStream[A]) {
    def map[B: Type](f: (A =>: A) => (A =>: B)): DataStream[B]             = Map(ds, f(Expression.Id()))
    def mapConcat[B: Type](f: (A =>: A) => (A =>: List[B])): DataStream[B] = MapConcat(ds, f(Expression.Id()))
    def filter(ds: DataStream[A])(f: (A =>: A) => (A =>: Boolean)): DataStream[A] =
      Filter(ds, f(Expression.Id()))
    def mapAccumulate[S: Type, B: Type](z: (Unit =>: S))(f: ((S, A) =>: (S, A)) => ((S, A) =>: (S, B))): DataStream[B] =
      MapAccumulate(ds, z, f(Expression.Id()))
    def groupBy[K: Type](
      f: (A =>: A) => (A =>: K)
    )(implicit computeGrouping: ComputeGrouping[A, K]): DataStream[computeGrouping.Out] =
      GroupBy[A, K, computeGrouping.Out](ds, f(Expression.Id()), computeGrouping)
    def assignTimestamps(f: (A =>: A) => (A =>: Long)): DataStream[Timestamped[A]] =
      AssignTimestamps(ds, f(Expression.Id()))
  }

  implicit class GroupedOps[K, V](ds: DataStream[Grouped[K, V]]) {
    def fold[R: Type](f: (Group[K, V] =>: Group[K, V]) => (Group[K, V] =>: R)): DataStream[R] =
      Fold(ds, f(Expression.Id()))
    def mapValues[B: Type](f: (V =>: V) => (V =>: B)): DataStream[Grouped[K, B]] =
      MapValues(ds, f(Expression.Id()))
  }

  implicit class GroupedTimestampedOps[K, V](ds: DataStream[Grouped[K, Timestamped[V]]]) {
    def foldWindow[S: Type](window: WindowAssigner, z: Unit =>: S)(
      f: ((S, Window, V) =>: (S, Window, V)) => ((S, Window, V) =>: S)
    ): DataStream[Grouped[K, Windowed[S]]] =
      FoldWindow(ds, window, z, f(Expression.Id()))
  }

  def fromLiterals[A](as: A*)(implicit A: Type[A]): DataStream[A] =
    DataStream.Literals(Chunk.fromIterable(as.map(A.lift)))
}
