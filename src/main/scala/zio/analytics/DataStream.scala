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

  implicit class Ops[A](ds: DataStream[A]) {
    def map[B: Type](f: (A =>: A) => (A =>: B)): DataStream[B]             = Map(ds, f(Expression.Id()))
    def mapConcat[B: Type](f: (A =>: A) => (A =>: List[B])): DataStream[B] = MapConcat(ds, f(Expression.Id()))
    def filter(ds: DataStream[A])(f: (A =>: A) => (A =>: Boolean)): DataStream[A] =
      Filter(ds, f(Expression.Id()))
    def mapAccumulate[S: Type, B: Type](z: (Unit =>: S))(f: ((S, A) =>: (S, A)) => ((S, A) =>: (S, B))): DataStream[B] =
      MapAccumulate(ds, z, f(Expression.Id()))
  }

  def fromLiterals[A](as: A*)(implicit A: Type[A]): DataStream[A] =
    DataStream.Literals(Chunk.fromIterable(as.map(A.lift)))
}
