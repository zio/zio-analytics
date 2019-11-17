package zio.analytics.planner

sealed abstract class Expression
object Expression {
  case object Id                                   extends Expression
  case class Compose(f: Expression, g: Expression) extends Expression
  case class FanOut(f: Expression, g: Expression)  extends Expression
  case class Split(f: Expression, g: Expression)   extends Expression
  case class LongLiteral(l: Long)                  extends Expression
  case class StringLiteral(s: String)              extends Expression
  case class BooleanLiteral(b: Boolean)            extends Expression

  case object Mul   extends Expression
  case object Sum   extends Expression
  case object Split extends Expression

  case class NthColumn(n: Int) extends Expression
}
