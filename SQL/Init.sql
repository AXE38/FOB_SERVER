--Создание пользователя-посредника
create user abs with password 'ABS';

--Инициализация схемы и пользователя
create schema "ABS" authorization postgres;
ALTER SCHEMA "ABS" OWNER TO abs;
grant all on schema "ABS" to abs;
alter role "abs" set search_path = 'ABS';

--Инициализация сессии
set session authorization "abs";
set search_path = 'ABS';

create sequence MAIN_SEQ
as bigint
start with 1
increment by 1;

create table DICT_ALL (
	id bigint PRIMARY key default nextval('MAIN_SEQ'),
	group_code varchar(16),
	code varchar(16),
	description varchar(255)
);

insert into DICT_ALL (group_code, code, description) values ('LOAN_TYPE', 'CRED', 'Кредит');
insert into DICT_ALL (group_code, code, description) values ('LOAN_TYPE', 'CC', 'Кредитная карта');
insert into DICT_ALL (group_code, code, description) values ('LOAN_TYPE', 'DC', 'Дебетовая карта');

insert into DICT_ALL (group_code, code, description) values ('DB_RESULT', '200', 'ОК');
insert into DICT_ALL (group_code, code, description) values ('DB_RESULT', '-1', 'Запрос не содержит XML');
insert into DICT_ALL (group_code, code, description) values ('DB_RESULT', '-2', 'Запрос содержит некорректный XML');
insert into DICT_ALL (group_code, code, description) values ('DB_RESULT', '-3', 'Неправильный логин или пароль');

insert into DICT_ALL (group_code, code, description) values ('USER_ROLE', 'ADMIN', 'Администратор');
insert into DICT_ALL (group_code, code, description) values ('USER_ROLE', 'USER', 'Пользователь');

create table ABS_USER (
	id bigint PRIMARY key default nextval('MAIN_SEQ'),
	first_name varchar(255),
	last_name varchar(255),
	middle_name varchar(255),
	position varchar(255),
	role_id bigint references DICT_ALL,
	abs_login varchar(16),
	abs_password varchar(255),
	token varchar(255),
	last_login_date timestamp
);

--пароль ADMIN
insert into ABS_USER (first_name, last_name, middle_name, position, role_id, abs_login, abs_password, token, last_login_date) values ('Администратор системы', NULL, NULL, NULL, (select id from DICT_ALL where code = 'ADMIN'), 'ADMIN', '835d6dc88b708bc646d6db82c853ef4182fabbd4a8de59c213f2b5ab3ae7d9be', NULL, NULL);

create table CLIENT (
	id bigint primary key default nextval('MAIN_SEQ'),
	first_name varchar(255),
	last_name varchar(255),
	middle_name varchar(255),
	date_of_birth date,
	pass varchar(255),
	phone varchar(255),
	create_date timestamp default current_timestamp,
	create_user bigint references ABS_USER,
	last_update_date timestamp default null,
	last_update_user bigint references ABS_USER default null
);

insert into CLIENT (first_name, last_name, middle_name, pass, phone, create_user) values ('Иван', 'Иванов', 'Иванович', '2564111111', '+711111111', (select id from ABS_USER where abs_login = 'ADMIN'));
insert into CLIENT (first_name, last_name, middle_name, pass, phone, create_user) values ('Дарья', 'Иванова', 'Алексеевна', '257711111', '+711111112', (select id from ABS_USER where abs_login = 'ADMIN'));

create table ACCOUNT (
	id bigint PRIMARY key default nextval('MAIN_SEQ'),
	collection_id bigint references CLIENT,
	num varchar(255),
	agreement_id varchar(255),
	card_pan varchar(255),
	loan_type bigint references DICT_ALL,
	iss_s decimal(15,2),
	s decimal(15,2),
	open_date timestamp,
	plan_close_date timestamp,
	close_date timestamp default  null,
	create_date timestamp default current_timestamp,
	create_user bigint references ABS_USER,
	last_update_date timestamp default null,
	last_update_user bigint references ABS_USER default null
);

insert into ACCOUNT (
	  collection_id
	, num
	, agreement_id
	, card_pan
	, loan_type
	, iss_s
	, s
	, open_date
	, plan_close_date
	, create_user
) values (
	  (select id from CLIENT fetch next 1 rows only)
	, '408171235488618831'
	, '15649866'
	, null
	, 1
	, 200000.00
	, 99000.00
	, '2022-02-10 12:00:00'
	, '2023-02-09 00:00:00'
	, (select id from ABS_USER where abs_login = 'ADMIN'));

create view VW_ACCOUNT as
	select 
		  ac.id
		, ac.collection_id
		, trim(cl.last_name || ' ' || cl.first_name || ' ' || cl.middle_name) client_name
		, ac.num
		, ac.agreement_id
		, da.description
		, ac.iss_s
		, ac.s
		, ac.create_date
		, ac.open_date
		, ac.plan_close_date
		, ac.close_date
	from ACCOUNT ac
	join CLIENT cl on cl.id = ac.collection_id
	join DICT_ALL da on da.id = ac.loan_type;

create table ACCOUNT_PLAN (
	collection_id bigint references ACCOUNT,
	dt timestamp,
	od_s decimal(15,2),
	prc_s decimal(15,2),
	create_date timestamp default current_timestamp,
	create_user bigint references ABS_USER,
	last_update_date timestamp default null,
	last_update_user bigint references ABS_USER default null
);

--История изменения реквизитов клиента
create table ETL_CLIENT (
	id bigint,
	first_name varchar(255),
	last_name varchar(255),
	middle_name varchar(255),
	date_of_birth date,
	pass varchar(255),
	phone varchar(255),
	create_date timestamp,
	create_user bigint,
	last_update_date timestamp,
	last_update_user bigint,
	ctl_action varchar(1)
);

create or replace function fun_trg_client() returns trigger as $trg_client$
begin
	if (tg_op = 'DELETE') then
		insert into "ABS".ETL_CLIENT (id, first_name, last_name, middle_name, pass, phone, create_date, create_user, last_update_date, last_update_user, ctl_action)
			select old.id, old.first_name, old.last_name, old.middle_name, old.pass, old.phone, old.create_date, old.create_user, old.last_update_date, old.last_update_user, 'D';
		return old;
	elsif (TG_OP = 'UPDATE') then 
		if current_setting('abs_cfg.update_before_delete') = '' then 
			insert into "ABS".ETL_CLIENT (id, first_name, last_name, middle_name, pass, phone, create_date, create_user, last_update_date, last_update_user, ctl_action)
				select new.id, new.first_name, new.last_name, new.middle_name, new.pass, new.phone, new.create_date, new.create_user, new.last_update_date, new.last_update_user, 'U';
			return new;
		end if;
	elsif (TG_OP = 'INSERT') then 
		insert into "ABS".ETL_CLIENT (id, first_name, last_name, middle_name, pass, phone, create_date, create_user, last_update_date, last_update_user, ctl_action)
			select new.id, new.first_name, new.last_name, new.middle_name, new.pass, new.phone, new.create_date, new.create_user, new.last_update_date, new.last_update_user, 'I';
		return new;
	end if;
	return null;
end;
$trg_client$ language plpgsql;

create trigger trg_client
	after insert or update or delete on client 
	for each row execute procedure fun_trg_client();

--История изменения счета
create table ETL_ACCOUNT (
	id bigint,
	collection_id bigint,
	num varchar(255),
	agreement_id varchar(255),
	card_pan varchar(255),
	loan_type bigint,
	iss_s decimal(15,2),
	s decimal(15,2),
	open_date timestamp,
	plan_close_date timestamp,
	close_date timestamp,
	create_date timestamp,
	create_user bigint,
	last_update_date timestamp,
	last_update_user bigint,
	interest_rate decimal(15,2),
	ctl_action varchar(1)
);

alter table ACCOUNT add interest_rate decimal(15,2)

create or replace function fun_trg_account() returns trigger as $trg_account$
begin
	if (tg_op = 'DELETE') then
		insert into "ABS".ETL_ACCOUNT
			select old.*, 'D';
		return old;
	elsif (TG_OP = 'UPDATE') then 
		if current_setting('abs_cfg.update_before_delete') = '' then 
			insert into "ABS".ETL_ACCOUNT
				select new.*, 'U';
			return new;
		end if;
	elsif (TG_OP = 'INSERT') then 
		insert into "ABS".ETL_ACCOUNT
			select new.*, 'I';
		return new;
	end if;
	return null;
end;
$trg_account$ language plpgsql;

create trigger trg_account
	after insert or update or delete on account
	for each row execute procedure fun_trg_account();

--История изменения остатков
create table ETL_ACCOUNT_PLAN (
	collection_id bigint,
	dt timestamp,
	od_s decimal(15,2),
	prc_s decimal(15,2),
	create_date timestamp,
	create_user bigint,
	last_update_date timestamp,
	last_update_user bigint,
	ctl_action varchar(1)
);

create or replace function fun_trg_account_plan() returns trigger as $trg_account_plan$
begin
	if (tg_op = 'DELETE') then
		insert into "ABS".ETL_ACCOUNT_PLAN
			select old.*, 'D';
		return old;
	elsif (TG_OP = 'UPDATE') then 
		if current_setting('abs_cfg.update_before_delete') = '' then 
			insert into "ABS".ETL_ACCOUNT_PLAN
				select new.*, 'U';
			return new;
		end if;
	elsif (TG_OP = 'INSERT') then 
		insert into "ABS".ETL_ACCOUNT_PLAN
			select new.*, 'I';
		return new;
	end if;
	return null;
end;
$trg_account_plan$ language plpgsql;

create trigger trg_account_plan
	after insert or update or delete on account_plan
	for each row execute procedure fun_trg_account_plan();

create table ABS_ROLE_RIGHTS (
	collection_id bigint references DICT_ALL,
	entity_type varchar(16),
	r_select int,
	r_update int,
	r_insert int,
	r_delete int
);

--генерация графика платежей
create or replace procedure "ABS".generate_account_plan
(
	p_account_id bigint
)
language plpgsql
as $$
declare
	v_mon_payment decimal(15,2);
	v_mon_int_rate float;
	v_sr decimal(15, 2);
	v_c record;
	v_od decimal(15, 2);
	v_prc decimal(15, 2);
	v_ost_od decimal(15, 2);
	v_idx integer := 1;
	v_dt date;
begin 
	delete from "ABS".account_plan
	where collection_id = p_account_id;
	for v_c in (select ac.id, ac.iss_s, ac.interest_rate, ac.open_date, ac.plan_close_date from "ABS".account ac where ac.loan_type = 1 and ac.id = p_account_id) loop
		v_mon_int_rate := v_c.interest_rate / 12 / 100;
		v_sr := (DATE_PART('year', v_c.plan_close_date) - DATE_PART('year', v_c.open_date)) * 12 + (DATE_PART('month', v_c.plan_close_date) - DATE_PART('month', v_c.open_date));
		v_sr := v_sr - 1;
		v_mon_payment := v_c.iss_s * ((v_mon_int_rate * power((1.0 + v_mon_int_rate), v_sr))/(power((1.0 + v_mon_int_rate), v_sr) - 1.0));
		v_ost_od := v_c.iss_s;
		v_dt := v_c.open_date + interval '1 month';
		v_idx := 1;
		while v_idx <= v_sr loop 
			v_prc := v_ost_od * v_mon_int_rate;
			v_od := v_mon_payment - v_prc;
			v_ost_od := v_ost_od - v_od;
			raise notice 'v_ost_od - v_od=%', v_ost_od - v_od;
			insert into "ABS".account_plan (collection_id, dt, od_s, prc_s, create_user) values (v_c.id, v_dt, v_od, v_prc, 10);
			v_dt := v_dt + interval '1 month';
			v_idx := v_idx + 1;
		end loop;
	end loop;
end$$;

