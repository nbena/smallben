create table if not exists jobs
(
    -- the test rule id
    id
    bigint
    primary
    key,
    -- the id of the group owning this job
    group_id
    bigint
    not
    null
    unique,
    -- the id of the supergroup owning this group of job
    super_group_id
    integer
    not
    null
    unique,
    -- set to true if you want to pause this test
    paused
    boolean
    not
    null
    default
    false,
    -- cron expression deciding the scheduling of this
    -- job
    cron_expression
    varchar
(
    256
) not null,
    -- id of the cron entry
    cron_id integer not null default 0,
    -- serialized job using base64(gob(job))
    -- it uses a `text` type instead of a binary,
    -- to make the Go struct more transparent to the underlying db.
    serialized_job text not null,
    -- serialized job input using base64(gob(job))
    -- it uses a `text` type instead of a binary,
    -- to make the Go struct more transparent to the underlying db.
    serialized_job_input text not null,
    -- when the item has been created
    created_at timestamp with time zone not null default current_timestamp,
    -- when the item has been updated last time
    updated_at timestamp  with time zone not null default current_timestamp,
);

-- index on the paused field
create index if not exists paused_idx on jobs(paused);
-- index on the group id
create index if not exists group_idx on jobs(group_id);
-- index on the super group id
create index if not exists super_group_idx on jobs(super_group_id);