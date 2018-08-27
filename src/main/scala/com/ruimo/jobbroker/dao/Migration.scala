package com.ruimo.jobbroker.dao

import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import liquibase.Liquibase
import liquibase.Contexts
import java.sql.Connection

object Migration {
  def perform(conn: Connection) {
    val database = DatabaseFactory.getInstance.findCorrectDatabaseImplementation(new JdbcConnection(conn))
      (new Liquibase("migration.sql", new ClassLoaderResourceAccessor(), database)).update(new Contexts())
  }
}
