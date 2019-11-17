package zio.analytics.planner

sealed abstract class Reified
object Reified {
  case object Unknown                         extends Reified
  case object Boolean                         extends Reified
  case object String                          extends Reified
  case object Long                            extends Reified
  case class Tuple2(_1: Reified, _2: Reified) extends Reified
}
