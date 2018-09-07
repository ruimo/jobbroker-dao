package com.ruimo.jobbroker.dao

import java.io.{ByteArrayInputStream, DataInputStream, InputStream}

import org.specs2.mutable._
import java.sql.DriverManager
import java.time.Instant

import com.ruimo.jobbroker.JobId

import scala.util.Random
import anorm._

class RequestsSpec extends Specification {
  "Requests" should {
    "Can create job by an array of byte." in new WithInMemoryDb {
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
    }

    "Can create job by a stream." in new WithInMemoryDb {
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
    }

    "Retrieve job fail if no job is found." in new WithInMemoryDb {
      Request.retrieveJobWithBytes(JobId(1L)) must throwA[JobNotFoundException]
      Request.storeJobResultWithBytes(JobId(1L), new Array[Byte](1)) must throwA[JobNotFoundException]
      Request.retrieveJobResultWithBytes(JobId(1L)) must throwA[JobNotFoundException]
    }

    "Obsolete records are deleted." in new WithInMemoryDb {
      def updateStatus(req: Request, jobStatus: JobStatus, startTime: Option[Instant] = None, endTime: Option[Instant] = None) {
        SQL(
          "update jobbroker_requests set job_status = {jobStatus}" +
            startTime.map(st => ", job_start_time = {startTime}").getOrElse("") +
            endTime.map(et => ", job_end_time = {endTime}").getOrElse("") +
            " where job_id={id}"
        ).on(
          (
            Seq(
              NamedParameter("jobStatus", jobStatus.code),
              NamedParameter("id", req.id.value)
            ) ++ (
              startTime.map(st => NamedParameter("startTime", st))
            ) ++ (
              endTime.map(et => NamedParameter("endTime", et))
            )
          ): _*
        )
      }

      val req00 = Request.submitJobWithBytes(AccountId("acc01"), ApplicationId("app01"), Array[Byte](0), Instant.ofEpochSecond(111L))
      val req01 = Request.submitJobWithBytes(AccountId("acc01"), ApplicationId("app01"), Array[Byte](0), Instant.ofEpochSecond(112L))
      val req02 = Request.submitJobWithBytes(AccountId("acc01"), ApplicationId("app01"), Array[Byte](0), Instant.ofEpochSecond(113L))

      val req10 = Request.submitJobWithBytes(AccountId("acc01"), ApplicationId("app01"), Array[Byte](0), Instant.ofEpochSecond(111L))
      updateStatus(req10, JobStatus.JobRunning)

      val req11 = Request.submitJobWithBytes(AccountId("acc01"), ApplicationId("app01"), Array[Byte](0), Instant.ofEpochSecond(112L))
      updateStatus(req11, JobStatus.JobRunning)
      val req12 = Request.submitJobWithBytes(AccountId("acc01"), ApplicationId("app01"), Array[Byte](0), Instant.ofEpochSecond(113L))
      updateStatus(req12, JobStatus.JobRunning)

      val req20 = Request.submitJobWithBytes(AccountId("acc01"), ApplicationId("app01"), Array[Byte](0), Instant.ofEpochSecond(111L))
      updateStatus(req20, JobStatus.JobEnded)
      val req21 = Request.submitJobWithBytes(AccountId("acc01"), ApplicationId("app01"), Array[Byte](0), Instant.ofEpochSecond(112L))
      updateStatus(req21, JobStatus.JobEnded)
      val req22 = Request.submitJobWithBytes(AccountId("acc01"), ApplicationId("app01"), Array[Byte](0), Instant.ofEpochSecond(113L))
      updateStatus(req22, JobStatus.JobEnded)

      val req30 = Request.submitJobWithBytes(AccountId("acc01"), ApplicationId("app01"), Array[Byte](0), Instant.ofEpochSecond(111L))
      updateStatus(req30, JobStatus.JobSystemError)
      val req31 = Request.submitJobWithBytes(AccountId("acc01"), ApplicationId("app01"), Array[Byte](0), Instant.ofEpochSecond(112L))
      updateStatus(req31, JobStatus.JobSystemError)
      val req32 = Request.submitJobWithBytes(AccountId("acc01"), ApplicationId("app01"), Array[Byte](0), Instant.ofEpochSecond(113L))
      updateStatus(req32, JobStatus.JobSystemError)

      val now = Instant.ofEpochSecond(1234L)

      Request.deleteObsoleteRecords(
        1234L - 112L,
        1234L - 223L,
        1234L - 334L,
        now
      ) === 4

      val ids = SQL("select job_id from jobbroker_requests order by job_id").as(SqlParser.scalar[Long] *).toSet
      ids.size === 8
      ids.contains(req01.id.value) === true
      ids.contains(req02.id.value) === true
      ids.contains(req11.id.value) === true
      ids.contains(req12.id.value) === true
      ids.contains(req21.id.value) === true
      ids.contains(req22.id.value) === true
      ids.contains(req31.id.value) === true
      ids.contains(req32.id.value) === true
    }
  }
}
