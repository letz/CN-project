DROP TABLE q1q2;
DROP TABLE q3;

CREATE TABLE q1q2
  (t_id varchar(40) NOT NULL,
  log_date timestamp NOT NULL,
  weather integer NOT NULL,
  sum_weight integer NOT NULL,
  max_ws integer NOT NULL);

CREATE TABLE q3
  (b_id varchar(40) NOT NULL unique,
  last_seen_at timestamp NOT NULL,
  primary key(b_id));


insert into q1q2	values ('tower-0', TIMESTAMP '2011-05-16 15:36:38', 1, 10, 20 );
insert into q1q2	values ('tower-1', TIMESTAMP '2011-05-19 15:36:38', 1, 1, 30 );
insert into q1q2	values ('tower-2', TIMESTAMP '2011-06-16 15:36:38', 1, 2, 15 );
insert into q1q2	values ('tower-2', TIMESTAMP '2013-05-16 15:36:38', 1, 3, 43 );
insert into q1q2	values ('tower-1', TIMESTAMP '2011-02-16 15:36:38', 1, 6, 3 );
insert into q1q2	values ('tower-1', TIMESTAMP '2011-05-16 15:36:38', 1, 12, 4 );
insert into q1q2	values ('tower-0', TIMESTAMP '2011-10-16 15:36:38', 1, 20, 40 );
insert into q1q2	values ('tower-0', TIMESTAMP '2011-05-16 15:36:38', 1, 10, 7 );


insert into q3 values ('bird-1', TIMESTAMP '2011-05-16 15:36:38');
insert into q3 values ('bird-2', TIMESTAMP '2011-05-16 15:36:38');
insert into q3 values ('bird-3', TIMESTAMP '2011-05-16 15:36:38');
insert into q3 values ('bird-4', TIMESTAMP '2011-11-16 15:36:38');
