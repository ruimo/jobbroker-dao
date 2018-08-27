package com.ruimo.jobbroker.dao

import java.sql.Connection
import scala.collection.{immutable => imm}
import java.time.Instant

import anorm._
import scala.language.postfixOps

case class Version(
  val longValue: Long
)

case class Settings(
  version: Version
)

object Settings {
  val simple = {
    SqlParser.get[Long]("jobbroker_settings.version") map {
      case ver =>
        Settings(Version(ver))
    }
  }

  def apply()(implicit conn: Connection): Settings = SQL(
    """
    select * from jobbroker_settings
    """
  ).as(simple.single)
}
