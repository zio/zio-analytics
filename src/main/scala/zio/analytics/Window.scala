package zio.analytics

import zio.duration.Duration

sealed abstract class Window
object Window {
  case class Fixed(size: Duration, step: Duration) extends Window
  case class Dynamic(gap: Duration)                extends Window

  def tumbling(size: Duration): Window                = Fixed(size, size)
  def sliding(size: Duration, step: Duration): Window = Fixed(size, step)
  def session(gap: Duration): Window                  = Dynamic(gap)
}
