create table if not exists tests (
    -- the test rule id
    id integer primary key,
    -- the id of the owning user evaluation rule
    user_evaluation_rule_id integer not null,
    -- the user_id owning this test
    user_id integer not null,
    -- set to true if you want to pause this test
    paused boolean not null default false,
    -- with an integer we can support intervals
    -- up to 2 years, so it's fine
    every_second integer not null check(every_second >= 60),
    -- id of the cron entry
    cron_id integer not null default 0,
    -- when the item has been created
    created_at timestamp with time zone not null default current_timestamp,
    -- when the item has been updated last time
    updated_at timestamp  with time zone not null default current_timestamp,
    unique(id, user_evaluation_rule_id, user_id)
);

-- index on the paused field
create index if not exists paused_idx on tests(paused);
-- index on the user evaluation rule id
create index if not exists user_evaluation_rule_idx on tests(user_evaluation_rule_id);
-- index on the user id
create index if not exists user_id_idx on tests(user_id);