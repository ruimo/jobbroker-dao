package com.ruimo.jobbroker.dao

import org.specs2.mutable._
import java.sql.DriverManager

class SettingsSpec extends Specification {
  "Settings" should {
    "Can retrieve content." in {
      org.h2.Driver.load()
      implicit val conn = DriverManager.getConnection("jdbc:h2:mem:test")
      Migration.perform(conn)

      val settings = Settings()
      settings.version.longValue === 0
    }
  }
}
