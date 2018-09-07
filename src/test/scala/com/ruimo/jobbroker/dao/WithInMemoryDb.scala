package com.ruimo.jobbroker.dao

import org.specs2.specification.Scope
import org.specs2.mutable._
import java.sql.DriverManager
import scala.util.Random

trait WithInMemoryDb extends Scope with After{
  implicit val conn = {
    org.h2.Driver.load()
    val c = DriverManager.getConnection("jdbc:h2:mem:test" + Random.nextLong())
    Migration.perform(c)
    c
  }

  def after = conn.close()
}
