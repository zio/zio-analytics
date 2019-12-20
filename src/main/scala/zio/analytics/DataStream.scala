package zio.analytics

import zio.Chunk

sealed abstract class DataStream[A]
object DataStream {
  case class Literals[A](data: Chunk[Expression[Unit, A]])                 extends DataStream[A]
  case class Map[A, B](ds: DataStream[A], f: Expression[A, B])             extends DataStream[B]
  case class MapConcat[A, B](ds: DataStream[A], f: Expression[A, List[B]]) extends DataStream[B]
  case class Filter[A](ds: DataStream[A], f: Expression[A, Boolean])       extends DataStream[A]
  case class MapAccumulate[S, A, B](ds: DataStream[A], z: Expression[Unit, S], f: Expression[(S, A), (S, B)])
      extends DataStream[B]
  case class GroupBy[A, K, V](ds: DataStream[A], f: Expression[A, (K, V)])               extends DataStream[Grouped[K, V]]
  case class Fold[K, V, R](ds: DataStream[Grouped[K, V]], f: Expression[Group[K, V], R]) extends DataStream[R]

  implicit class Ops[A](ds: DataStream[A]) {
    def map[B: Type](f: (A =>: A) => (A =>: B)): DataStream[B]             = Map(ds, f(Expression.Id()))
    def mapConcat[B: Type](f: (A =>: A) => (A =>: List[B])): DataStream[B] = MapConcat(ds, f(Expression.Id()))
    def filter(ds: DataStream[A])(f: (A =>: A) => (A =>: Boolean)): DataStream[A] =
      Filter(ds, f(Expression.Id()))
    def mapAccumulate[S: Type, B: Type](z: (Unit =>: S))(f: ((S, A) =>: (S, A)) => ((S, A) =>: (S, B))): DataStream[B] =
      MapAccumulate(ds, z, f(Expression.Id()))
    def groupBy[K: Type, V: Type](f: (A =>: A) => (A =>: (K, V))): DataStream[Grouped[K, V]] =
      GroupBy(ds, f(Expression.Id()))
  }

  implicit class GroupedOps[K, V](ds: DataStream[Grouped[K, V]]) {
    def fold[R: Type](f: (Group[K, V] =>: Group[K, V]) => (Group[K, V] =>: R)): DataStream[R] =
      Fold(ds, f(Expression.Id()))
  }

  def fromLiterals[A](as: A*)(implicit A: Type[A]): DataStream[A] =
    DataStream.Literals(Chunk.fromIterable(as.map(A.lift)))
}
