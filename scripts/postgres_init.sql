create table if not exists user_evaluation_rules (
    -- the user evaluation rule id
    id integer primary key,
    -- the user_id owning this user evaluation rule
    user_id integer not null,
    -- when the item has been created
    created_at timestamp,
    -- when the item has been updated last time
    updated_at timestamp,
    unique(id, user_id)
);

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
    created_at timestamp,
    -- when the item has been updated last time
    updated_at timestamp,
    constraint test_fk_uer foreign key (user_evaluation_rule_id)
        references user_evaluation_rules(id) on update cascade on delete cascade,
    unique(id, user_evaluation_rule_id, user_id)
);
