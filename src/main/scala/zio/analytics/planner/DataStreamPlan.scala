package zio.analytics.planner

import zio.Chunk

sealed abstract class DataStreamPlan
object DataStreamPlan {
  case class Literals(data: Chunk[Expression])                               extends DataStreamPlan
  case class Map(ds: DataStreamPlan, f: Expression)                          extends DataStreamPlan
  case class MapConcat(ds: DataStreamPlan, f: Expression)                    extends DataStreamPlan
  case class Filter(ds: DataStreamPlan, f: Expression)                       extends DataStreamPlan
  case class MapAccumulate(ds: DataStreamPlan, z: Expression, f: Expression) extends DataStreamPlan
}
