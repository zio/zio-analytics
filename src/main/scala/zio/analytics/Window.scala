package zio.analytics

import zio.duration.Duration

sealed abstract class WindowAssigner {
  def assign(timestamp: Long): List[Window]
}
object WindowAssigner {
  case class Fixed(size: Duration, step: Duration) extends WindowAssigner {
    def assign(timestamp: Long): List[Window] = {
      val sizeMillis = size.toMillis
      val firstStart = (timestamp / sizeMillis) * sizeMillis

      (firstStart until (firstStart + sizeMillis) by step.toMillis)
        .filter(bound => bound <= timestamp && bound + sizeMillis > timestamp)
        .map { lowerBound =>
          Window(lowerBound, lowerBound + sizeMillis - 1)
        }
        .toList
    }
  }

  case class Dynamic(gap: Duration) extends WindowAssigner {
    // TODO
    def assign(timestamp: Long): List[Window] = List()
  }

  def tumbling(size: Duration): WindowAssigner                = Fixed(size, size)
  def sliding(size: Duration, step: Duration): WindowAssigner = Fixed(size, step)
  def session(gap: Duration): WindowAssigner                  = Dynamic(gap)
}

case class Window(lower: Long, upper: Long)
case class Windowed[A](window: Window, value: A)
