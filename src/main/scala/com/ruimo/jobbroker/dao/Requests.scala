package com.ruimo.jobbroker.dao

import java.io.InputStream
import java.sql.Connection
import java.time.Instant

import anorm._
import scala.language.postfixOps

case class RequestId(value: Long) extends AnyVal

case class AccountId(value: String) extends AnyVal

case class ApplicationId(value: String) extends AnyVal

case class Request(
  id: RequestId,
  accountId: AccountId,
  applicationId: ApplicationId,
  jobStatus: JobStatus,
  acceptedTime: Instant,
  jobStartTime: Option[Instant],
  jobEndTime: Option[Instant]
)

sealed trait JobStatus {
  val code: Long
}

class JobNotFoundException(val requestId: RequestId) extends RuntimeException(
  "This request is already taken or not exists (id=" + requestId + ")"
)

object JobStatus {
  def apply(code: Long): JobStatus = Table.find(_.code == code).getOrElse {
    throw new IllegalArgumentException("Job status code (=" + code + ") is invalid.")
  }

  val Table: List[JobStatus] = List(
    JobQueued, JobRunning, JobEnded
  )

  case object JobQueued extends JobStatus {
    val code =  0
  }

  case object JobRunning extends JobStatus {
    val code = 1
  }

  case object JobEnded extends JobStatus {
    val code = 2
  }
}

object Request {
  val simple = {
    SqlParser.get[Long]("jobbroker_requests.request_id") ~
    SqlParser.get[String]("jobbroker_requests.account_id") ~
    SqlParser.get[String]("jobbroker_requests.application_id") ~
    SqlParser.get[Long]("jobbroker_requests.job_status") ~
    SqlParser.get[Instant]("jobbroker_requests.accepted_time") ~
    SqlParser.get[Option[Instant]]("jobbroker_requests.job_start_time") ~
    SqlParser.get[Option[Instant]]("jobbroker_requests.job_end_time") map {
      case id~accId~appId~jobStatus~acceptedTime~jobStartTime~jobEndTime =>
        Request(
          RequestId(id), AccountId(accId), ApplicationId(appId),
          JobStatus(jobStatus),
          acceptedTime, jobStartTime, jobEndTime
        )
    }
  }

  val inputBytes: RowParser[Array[Byte]] = SqlParser.get[Array[Byte]]("jobbroker_requests.application_input")
  val inputStream: RowParser[InputStream] = SqlParser.get[InputStream]("jobbroker_requests.application_input")

  val outputBytes: RowParser[Array[Byte]] = SqlParser.get[Array[Byte]]("jobbroker_requests.application_output")
  val outputStream: RowParser[InputStream] = SqlParser.get[InputStream]("jobbroker_requests.application_output")

  val withInputBytes: RowParser[(Request, Array[Byte])] = simple ~ inputBytes map {
    case req~in => (req, in)
  }

  val withInputStream: RowParser[(Request, InputStream)] = simple ~ inputStream map {
    case req~in => (req, in)
  }

  val withOutputBytes: RowParser[(Request, Array[Byte])] = simple ~ outputBytes map {
    case req~out => (req, out)
  }

  val withOutputStream: RowParser[(Request, InputStream)] = simple ~ outputStream map {
    case req~out => (req, out)
  }

  def submitJob[T](
    accountId: AccountId, applicationId: ApplicationId, in: T, toParmeterValue: T => ParameterValue, now: Instant = Instant.now()
  )(implicit conn: Connection): Request = {
    SQL(
      """
      insert into jobbroker_requests (
        request_id, account_id, application_id, job_status, application_input, accepted_time
      ) values (
        (select nextval('jobbroker_requests_seq')),
        {accountId}, {applicationId}, {jobStatus}, {input}, {acceptedTime}
      )
      """
    ).on(
      'accountId -> accountId.value,
      'applicationId -> applicationId.value,
      'jobStatus -> JobStatus.JobQueued.code,
      'input -> toParmeterValue(in),
      'acceptedTime -> now
    ).executeUpdate()

    val id: Long = SQL("select currval('jobbroker_requests_seq')").as(SqlParser.scalar[Long].single)
    Request(RequestId(id), accountId, applicationId, JobStatus.JobQueued, now, None, None)
  }

  def submitJobWithBytes(
    accountId: AccountId, applicationId: ApplicationId, in: Array[Byte], now: Instant = Instant.now()
  )(implicit conn: Connection): Request = submitJob(
    accountId, applicationId, in, (input: Array[Byte]) => ParameterValue.toParameterValue(input), now
  )

  def submitJobWithStream(
    accountId: AccountId, applicationId: ApplicationId, in: InputStream, now: Instant = Instant.now()
  )(implicit conn: Connection): Request = submitJob(
    accountId, applicationId, in, (input: InputStream) => ParameterValue.toParameterValue(input), now
  )

  def retrieveJob[T](
    requestId: RequestId, rowParser: RowParser[(Request, T)], now: Instant = Instant.now()
  )(implicit conn: Connection): (Request, T) = {
    val updateCount = SQL(
      """
      update jobbroker_requests set
        job_status = {newJobStatus},
        job_start_time = {startTime}
      where request_id = {id} and job_status = {currentJobStatus}
      """
    ).on(
      'id -> requestId.value,
      'currentJobStatus -> JobStatus.JobQueued.code,
      'newJobStatus -> JobStatus.JobRunning.code,
      'startTime -> now
    ).executeUpdate()

    if (updateCount == 1) {
      SQL(
        """
        select * from jobbroker_requests where request_id = {id}
        """
      ).on(
        'id -> requestId.value
      ).as(rowParser.single)
    } else {
      throw new JobNotFoundException(requestId)
    }
  }

  def retrieveJobWithBytes(
    requestId: RequestId, now: Instant = Instant.now()
  )(implicit conn: Connection): (Request, Array[Byte]) = retrieveJob[Array[Byte]](
    requestId, withInputBytes, now
  )

  def retrieveJobWithStream(
    requestId: RequestId, now: Instant = Instant.now()
  )(implicit conn: Connection): (Request, InputStream) = retrieveJob[InputStream](
    requestId, withInputStream, now
  )

  def storeJobResult[T](
    requestId: RequestId, result: T, toParameterValue: T => ParameterValue, now: Instant = Instant.now()
  )(implicit conn: Connection): Request = {
    val updateCount = SQL(
      """
      update jobbroker_requests set
        job_status = {newJobStatus},
        job_end_time = {endTime},
        application_output = {output}
      where request_id = {id} and job_status = {currentJobStatus}
      """
    ).on(
      'newJobStatus -> JobStatus.JobEnded.code,
      'endTime -> now,
      'output -> toParameterValue(result),
      'id -> requestId.value,
      'currentJobStatus -> JobStatus.JobRunning.code
    ).executeUpdate()

    if (updateCount == 1) {
      SQL(
        """
        select * from jobbroker_requests where request_id = {id}
        """
      ).on(
        'id -> requestId.value
      ).as(simple.single)
    } else {
      throw new JobNotFoundException(requestId)
    }
  }

  def storeJobResultWithBytes(
    requestId: RequestId, result: Array[Byte], now: Instant = Instant.now()
  )(implicit conn: Connection): Request = storeJobResult(
    requestId, result, (result: Array[Byte]) => ParameterValue.toParameterValue(result), now
  )

  def storeJobResultWithStream(
    requestId: RequestId, result: InputStream, now: Instant = Instant.now()
  )(implicit conn: Connection): Request = storeJobResult(
    requestId, result, (result: InputStream) => ParameterValue.toParameterValue(result), now
  )

  def retrieveJobResult[T](
    requestId: RequestId, rowParser: RowParser[(Request, T)]
  )(implicit conn: Connection): (Request, T) = SQL(
    """
    select * from jobbroker_requests where request_id = {id}
    """
  ).on(
    'id -> requestId.value
  ).as(rowParser.singleOpt).getOrElse(
    throw new JobNotFoundException(requestId)
  )

  def retrieveJobResultWithBytes(
    requestId: RequestId
  )(implicit conn: Connection): (Request, Array[Byte]) = retrieveJobResult(
    requestId, withOutputBytes
  )

  def retrieveJobResultWithStream(
    requestId: RequestId
  )(implicit conn: Connection): (Request, InputStream) = retrieveJobResult(
    requestId, withOutputStream
  )
}
