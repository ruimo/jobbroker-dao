--liquibase formatted sql

--changeset shanai:1
create table jobbroker_requests (
  job_id bigint not null,
  account_id varchar(256) not null,
  application_id varchar(256) not null,
  job_status bigint not null,
  accepted_time timestamp,
  job_start_time timestamp,
  job_end_time timestamp,
  application_input bytea not null,
  application_output bytea
);

alter table jobbroker_requests add constraint pk_jobbroker_requests primary key (job_id);
create index ix_jobbroker_requests_account_id on jobbroker_requests (account_id);
create index ix_jobbroker_requests_application_id on jobbroker_requests (application_id);
create index ix_jobbroker_requests_accepted_time on jobbroker_requests (accepted_time);
create index ix_jobbroker_requests_job_start_time on jobbroker_requests (job_start_time);
create index ix_jobbroker_requests_job_end_time on jobbroker_requests (job_end_time);

create sequence jobbroker_requests_seq;

create table jobbroker_settings (
  version bigint not null
);

insert into jobbroker_settings (
  version
) values (
  0
)

--rollback drop table jobbroker_settings;
--rollback drop sequence jobbroker_requests_seq;
--rollback drop table jobbroker_requests;
