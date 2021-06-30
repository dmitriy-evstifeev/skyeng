insert into `order` (student_id, teacher_id, stage, status, created_at, updated_at)
	select distinct
		student_id,
		teacher_id,
		stage,
		status,
		created_at,
		updated_at
-- 		rand(),
-- 		md5(concat(student_id, teacher_id, stage, status, created_at, updated_at))
	from
		`order`
	order by
		rand()
	limit 1;