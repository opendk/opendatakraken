-- TEST
drop schema test.testuser cascade;

drop user testuser;

create user testuser with password 'test';

create schema test.testuser authorization testuser;

grant list on test to testuser;


-- SugarCRM
drop user sugarcrm;

drop schema testdb.sugarcrm cascade;

create user sugarcrm with password 'sugarcrm';

create schema testdb.sugarcrm authorization sugarcrm;

grant list on testdb to sugarcrm;


-- DWHStage
drop schema testdb.dwhstage cascade;

drop user dwhstage;

create user dwhstage with password 'dwhstage';

create schema testdb.dwhstage authorization dwhstage;

grant list on testdb to dwhstage;