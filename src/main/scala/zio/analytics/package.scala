package zio

import zio.analytics.planner.Planner

package object analytics {
  val api: Analytics = new Planner {}
}
