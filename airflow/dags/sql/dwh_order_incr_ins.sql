with
	prev_dump as (
		select
			p1.*,
			row_number() over (partition by row_hash order by id) as rn
		from (
			select	
				p.*,
				md5(concat(student_id, teacher_id, stage, status, created_at, updated_at)) as row_hash
			from
				temp.mysql_order_previous p
		) p1
	),
	
	curr_dump as (
		select 
			c1.*,
			row_number() over (partition by row_hash order by id) as rn
		from (
			select
				c.*,
				md5(concat(student_id, teacher_id, stage, status, created_at, updated_at)) as row_hash
			from
				temp.mysql_order_current c
		) c1
	),
	
	new_row_hashes as (
		select distinct
			row_hash
		from (
			select
				row_hash
			from
				curr_dump
			where
				rn = 1
			except
			select
				row_hash
			from
				prev_dump
			where
				rn = 1
		) t
	),
	
	rows_to_add as (
		select
			cd.id as order_id,
			cd.student_id,
			cd.teacher_id,
			cd.stage,
			cd.status,
			cd.row_hash,
			cd.created_at,
			cd.updated_at
		from
			curr_dump cd
		join
			new_row_hashes rh
			on cd.row_hash = rh.row_hash
		where
			rn = 1
	)
	

insert into public.raw_order
	select
		nextval('seq_raw_order'),
		r.*
	from
		rows_to_add r;
