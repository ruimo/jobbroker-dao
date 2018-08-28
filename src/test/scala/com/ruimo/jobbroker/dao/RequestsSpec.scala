package com.ruimo.jobbroker.dao

import java.io.{ByteArrayInputStream, DataInputStream, InputStream}

import org.specs2.mutable._
import java.sql.DriverManager
import java.time.Instant

import com.ruimo.jobbroker.JobId

import scala.util.Random

class RequestsSpec extends Specification {
  "Requests" should {
    "Can create job by an array of byte." in {
      org.h2.Driver.load()
      implicit val conn = DriverManager.getConnection("jdbc:h2:mem:test" + Random.nextLong())
      Migration.perform(conn)
      val req = Request.submitJobWithBytes(
        AccountId("account01"),
        ApplicationId("app01"),
        "Hello".getBytes("utf-8"),
        Instant.ofEpochMilli(123L)
      )

      req.accountId === AccountId("account01")
      req.applicationId === ApplicationId("app01")
      req.jobStatus === JobStatus.JobQueued
      req.acceptedTime === Instant.ofEpochMilli(123L)

      val (req02: Request, in: Array[Byte]) = Request.retrieveJobWithBytes(req.id, Instant.ofEpochMilli(234L))
      new String(in, "utf-8") === "Hello"
      req02.accountId === AccountId("account01")
      req02.applicationId === ApplicationId("app01")
      req02.jobStatus === JobStatus.JobRunning
      req02.acceptedTime === Instant.ofEpochMilli(123L)
      req02.jobStartTime === Some(Instant.ofEpochMilli(234L))

      val req03: Request = Request.storeJobResultWithBytes(req.id, (new String("Result").getBytes("utf-8")), Instant.ofEpochMilli(345L))
      req03.accountId === AccountId("account01")
      req03.applicationId === ApplicationId("app01")
      req03.jobStatus === JobStatus.JobEnded
      req03.acceptedTime === Instant.ofEpochMilli(123L)
      req03.jobStartTime === Some(Instant.ofEpochMilli(234L))
      req03.jobEndTime === Some(Instant.ofEpochMilli(345L))

      val (req04: Request, result: Array[Byte] ) = Request.retrieveJobResultWithBytes(req.id)
      req04.accountId === AccountId("account01")
      req04.applicationId === ApplicationId("app01")
      req04.jobStatus === JobStatus.JobEnded
      req04.acceptedTime === Instant.ofEpochMilli(123L)
      req04.jobStartTime === Some(Instant.ofEpochMilli(234L))
      req04.jobEndTime === Some(Instant.ofEpochMilli(345L))
      (new String(result, "utf-8")) === "Result"

      conn.close()
      1 === 1
    }

    "Can create job by a stream." in {
      org.h2.Driver.load()

      implicit val conn = DriverManager.getConnection("jdbc:h2:mem:test" + Random.nextLong())
      Migration.perform(conn)

      val is = new ByteArrayInputStream("Hello".getBytes("utf-8"))
      val req = Request.submitJobWithStream(
        AccountId("account01"),
        ApplicationId("app01"),
        is,
        Instant.ofEpochMilli(123L)
      )

      req.accountId === AccountId("account01")
      req.applicationId === ApplicationId("app01")
      req.jobStatus === JobStatus.JobQueued
      req.acceptedTime === Instant.ofEpochMilli(123L)

      val (req02: Request, in: InputStream) = Request.retrieveJobWithStream(req.id, Instant.ofEpochMilli(234L))
      val buf = new Array[Byte](5)
      new DataInputStream(in).readFully(buf)
      new String(buf, "utf-8") === "Hello"
      req02.accountId === AccountId("account01")
      req02.applicationId === ApplicationId("app01")
      req02.jobStatus === JobStatus.JobRunning
      req02.acceptedTime === Instant.ofEpochMilli(123L)
      req02.jobStartTime === Some(Instant.ofEpochMilli(234L))

      val req03: Request = Request.storeJobResultWithStream(req.id, new ByteArrayInputStream("Result".getBytes("utf-8")), Instant.ofEpochMilli(345L))
      req03.accountId === AccountId("account01")
      req03.applicationId === ApplicationId("app01")
      req03.jobStatus === JobStatus.JobEnded
      req03.acceptedTime === Instant.ofEpochMilli(123L)
      req03.jobStartTime === Some(Instant.ofEpochMilli(234L))
      req03.jobEndTime === Some(Instant.ofEpochMilli(345L))

      val (req04: Request, is02: InputStream) = Request.retrieveJobResultWithStream(req03.id)
      req04.accountId === AccountId("account01")
      req04.applicationId === ApplicationId("app01")
      req04.jobStatus === JobStatus.JobEnded
      req04.acceptedTime === Instant.ofEpochMilli(123L)
      req04.jobStartTime === Some(Instant.ofEpochMilli(234L))
      req04.jobEndTime === Some(Instant.ofEpochMilli(345L))
      val buf02 = new Array[Byte](6)
      new DataInputStream(is02).readFully(buf02)
      new String(buf02, "utf-8") === "Result"

      conn.close()
      1 === 1
    }

    "Retrieve job fail if no job is found." in {
      org.h2.Driver.load()
      implicit val conn = DriverManager.getConnection("jdbc:h2:mem:test" + Random.nextLong())
      Migration.perform(conn)
      Request.retrieveJobWithBytes(JobId(1L)) must throwA[JobNotFoundException]
      Request.storeJobResultWithBytes(JobId(1L), new Array[Byte](1)) must throwA[JobNotFoundException]
      Request.retrieveJobResultWithBytes(JobId(1L)) must throwA[JobNotFoundException]
      conn.close()
      1 === 1
    }
  }
}
