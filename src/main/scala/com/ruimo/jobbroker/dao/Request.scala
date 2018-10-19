package com.ruimo.jobbroker.dao

import java.io.InputStream
import java.sql.Connection
import java.time.Instant

import anorm._
import com.ruimo.jobbroker.JobId
import org.slf4j.{Logger, LoggerFactory}

import scala.language.postfixOps

case class AccountId(value: String) extends AnyVal

case class ApplicationId(value: String) extends AnyVal

trait Request {
  val id: JobId
  val accountId: AccountId
  val applicationId: ApplicationId
  val jobStatus: JobStatus
  val acceptedTime: Instant
  val jobStartTime: Option[Instant]
  val jobEndTime: Option[Instant]
}

case class RequestImpl(
  id: JobId,
  accountId: AccountId,
  applicationId: ApplicationId,
  jobStatus: JobStatus,
  acceptedTime: Instant,
  jobStartTime: Option[Instant],
  jobEndTime: Option[Instant]
) extends Request

sealed trait JobStatus {
  val code: Long
}

class JobNotFoundException(val jobId: JobId) extends RuntimeException(
  "This request is already taken or not exists (id=" + jobId + ")"
)

object JobStatus {
  def apply(code: Long): JobStatus = Table.find(_.code == code).getOrElse {
    throw new IllegalArgumentException("Job status code (=" + code + ") is invalid.")
  }

  val Table: List[JobStatus] = List(
    JobQueued, JobRunning, JobEnded, JobSystemError
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

  case object JobSystemError extends JobStatus {
    val code = 3
  }
}

object Request {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  val simple = {
    SqlParser.get[Long]("jobbroker_requests.job_id") ~
    SqlParser.get[String]("jobbroker_requests.account_id") ~
    SqlParser.get[String]("jobbroker_requests.application_id") ~
    SqlParser.get[Long]("jobbroker_requests.job_status") ~
    SqlParser.get[Instant]("jobbroker_requests.accepted_time") ~
    SqlParser.get[Option[Instant]]("jobbroker_requests.job_start_time") ~
    SqlParser.get[Option[Instant]]("jobbroker_requests.job_end_time") map {
      case id~accId~appId~jobStatus~acceptedTime~jobStartTime~jobEndTime =>
        RequestImpl(
          JobId(id), AccountId(accId), ApplicationId(appId),
          JobStatus(jobStatus),
          acceptedTime, jobStartTime, jobEndTime
        )
    }
  }

  val inputBytes: RowParser[Array[Byte]] = SqlParser.get[Array[Byte]]("jobbroker_requests.application_input")
  val inputStream: RowParser[InputStream] = SqlParser.get[InputStream]("jobbroker_requests.application_input")

  val outputBytes: RowParser[Option[Array[Byte]]] = SqlParser.get[Option[Array[Byte]]]("jobbroker_requests.application_output")
  val outputStream: RowParser[Option[InputStream]] = SqlParser.get[Option[InputStream]]("jobbroker_requests.application_output")

  val withInputBytes: RowParser[(Request, Array[Byte])] = simple ~ inputBytes map {
    case req~in => (req, in)
  }

  val withInputStream: RowParser[(Request, InputStream)] = simple ~ inputStream map {
    case req~in => (req, in)
  }

  val withOutputBytes: RowParser[(Request, Option[Array[Byte]])] = simple ~ outputBytes map {
    case req~out => (req, out)
  }

  val withOutputStream: RowParser[(Request, Option[InputStream])] = simple ~ outputStream map {
    case req~out => (req, out)
  }

  def submitJob[T](
    accountId: AccountId, applicationId: ApplicationId, in: T, toParmeterValue: T => ParameterValue, now: Instant = Instant.now()
  )(implicit conn: Connection): Request = {
    logger.info("submitJob(" + accountId + ", " + applicationId + ", " + in + ") called.")b

    SQL(
      """
      insert into jobbroker_requests (
        job_id, account_id, application_id, job_status, application_input, accepted_time
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
    RequestImpl(JobId(id), accountId, applicationId, JobStatus.JobQueued, now, None, None)
  }

  import anorm.ToStatement._

  def submitJobWithBytes(
    accountId: AccountId, applicationId: ApplicationId, in: Array[Byte], now: Instant = Instant.now()
  )(implicit conn: Connection): Request = submitJob(
    accountId, applicationId, in, (input: Array[Byte]) => ToParameterValue[Array[Byte]].apply(input), now
  )

  def submitJobWithStream(
    accountId: AccountId, applicationId: ApplicationId, in: InputStream, now: Instant = Instant.now()
  )(implicit conn: Connection): Request = submitJob(
    accountId, applicationId, in, (input: InputStream) => ToParameterValue[InputStream].apply(input), now
  )

  def retrieveJob[T](
    jobId: JobId, rowParser: RowParser[(Request, T)], now: Instant = Instant.now()
  )(implicit conn: Connection): (Request, T) = {
    logger.info("retrieveJob(" + jobId + ") called.")

    val updateCount = SQL(
      """
      update jobbroker_requests set
        job_status = {newJobStatus},
        job_start_time = {startTime}
      where job_id = {id} and job_status = {currentJobStatus}
      """
    ).on(
      'id -> jobId.value,
      'currentJobStatus -> JobStatus.JobQueued.code,
      'newJobStatus -> JobStatus.JobRunning.code,
      'startTime -> now
    ).executeUpdate()

    if (updateCount == 1) {
      SQL(
        """
        select * from jobbroker_requests where job_id = {id}
        """
      ).on(
        'id -> jobId.value
      ).as(rowParser.single)
    } else {
      throw new JobNotFoundException(jobId)
    }
  }

  def retrieveJobWithBytes(
    jobId: JobId, now: Instant = Instant.now()
  )(implicit conn: Connection): (Request, Array[Byte]) = retrieveJob[Array[Byte]](
    jobId, withInputBytes, now
  )

  def retrieveJobWithStream(
    jobId: JobId, now: Instant = Instant.now()
  )(implicit conn: Connection): (Request, InputStream) = retrieveJob[InputStream](
    jobId, withInputStream, now
  )

  def storeJobResult[T](
    jobId: JobId, result: T, toParameterValue: T => ParameterValue, now: Instant = Instant.now(),
    jobStatus: JobStatus = JobStatus.JobEnded
  )(implicit conn: Connection): Request = {
    logger.info("storeJobResult(" + jobId + ", " + jobStatus + ") called.")

    val updateCount = SQL(
      """
      update jobbroker_requests set
        job_status = {newJobStatus},
        job_end_time = {endTime},
        application_output = {output}
      where job_id = {id} and job_status = {currentJobStatus}
      """
    ).on(
      'newJobStatus -> jobStatus.code,
      'endTime -> now,
      'output -> toParameterValue(result),
      'id -> jobId.value,
      'currentJobStatus -> JobStatus.JobRunning.code
    ).executeUpdate()

    if (updateCount == 1) {
      SQL(
        """
        select * from jobbroker_requests where job_id = {id}
        """
      ).on(
        'id -> jobId.value
      ).as(simple.single)
    } else {
      throw new JobNotFoundException(jobId)
    }
  }

  def storeJobResultWithBytes(
    jobId: JobId, result: Array[Byte], now: Instant = Instant.now(),
    jobStatus: JobStatus = JobStatus.JobEnded
  )(implicit conn: Connection): Request = storeJobResult(
    jobId, result, (result: Array[Byte]) => ToParameterValue[Array[Byte]].apply(result), now, jobStatus
  )

  def storeJobResultWithStream(
    jobId: JobId, result: InputStream, now: Instant = Instant.now(),
    jobStatus: JobStatus = JobStatus.JobEnded
  )(implicit conn: Connection): Request = storeJobResult(
    jobId, result, (result: InputStream) => ToParameterValue[InputStream].apply(result), now, jobStatus
  )

  def retrieveJobResult[T](
    jobId: JobId, rowParser: RowParser[(Request, T)]
  )(implicit conn: Connection): (Request, T) = {
    logger.info("retrieveJobResult(" + jobId + ") called.")
    SQL(
      """
      select * from jobbroker_requests where job_id = {id}
      """
    ).on(
      'id -> jobId.value
    ).as(rowParser.singleOpt).getOrElse(
      throw new JobNotFoundException(jobId)
    )
  }

  def retrieveJobResultWithBytes(
    jobId: JobId
  )(implicit conn: Connection): (Request, Option[Array[Byte]]) = retrieveJobResult(
    jobId, withOutputBytes
  )

  def retrieveJobResultWithStream(
    jobId: JobId
  )(implicit conn: Connection): (Request, Option[InputStream]) = retrieveJobResult(
    jobId, withOutputStream
  )

  def deleteObsoleteRecords(
    notStartedTimeoutInSecs: Long = 3 * 60,
    notEndedTimeoutInSecs: Long = 10 * 60,
    endedTimeoutInSecs: Long = 30 * 60,
    now: Instant = Instant.now()
  )(implicit conn: Connection): Long = SQL(
    """
    delete from jobbroker_requests
    where (
      job_status = {queuedStatus} and
      extract(epoch from accepted_time) + {notStartedTimeoutInSecs} < extract(epoch from {now})
    ) or (
      job_status = {runningStatus} and
      extract(epoch from job_start_time) + {notEndedTimeoutInSecs} < extract(epoch from {now})
    ) or (
      (job_status = {endedStatus} or job_status = {systemErrorStatus}) and
      extract(epoch from job_start_time) + {endedTimeoutInSecs} < extract(epoch from {now})
    )
    """
  ).on(
    'queuedStatus -> JobStatus.JobQueued.code,
    'notStartedTimeoutInSecs -> notStartedTimeoutInSecs,
    'runningStatus -> JobStatus.JobRunning.code,
    'notEndedTimeoutInSecs -> notEndedTimeoutInSecs,
    'endedStatus -> JobStatus.JobEnded.code,
    'systemErrorStatus -> JobStatus.JobSystemError.code,
    'endedTimeoutInSecs -> endedTimeoutInSecs,
    'now -> now
  ).executeUpdate()
}
