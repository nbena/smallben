create table if not exists jobs (
    -- the test rule id
    id integer primary key,
    -- the id of the group owning this job
    group_id integer not null,
    -- the id of the supergroup owning this group of job
    super_group_id integer not null,
    -- set to true if you want to pause this test
    paused boolean not null default false,
    -- with an integer we can support intervals
    -- up to 2 years, so it's fine
    every_second integer not null check(every_second >= 60),
    -- id of the cron entry
    cron_id integer not null default 0,
    -- serialized job using gob
    serialized_job bytea not null,
    -- serialized input of the job using gob
    serialized_job_input bytea not null,
    -- when the item has been created
    created_at timestamp with time zone not null default current_timestamp,
    -- when the item has been updated last time
    updated_at timestamp  with time zone not null default current_timestamp,
    unique(id, group_id, super_group_id)
);

-- index on the paused field
create index if not exists paused_idx on tests(paused);
-- index on the group id
create index if not exists group_idx on tests(group_id);
-- index on the super group id
create index if not exists super_group_idx on tests(super_group_id);