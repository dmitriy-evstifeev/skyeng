insert into `order` (student_id, teacher_id, stage, status, created_at, updated_at) values (
	round(rand() * 100000),
	round(rand() * 10000),
	elt(round(rand() * 10) % 3 + 1 , 'finished', 'abandoned', 'cancelled'),
	elt(round(rand() * 10) % 3 + 1 , 'successful', 'unsuccessful', 'unknown'),
	current_timestamp,
	current_timestamp + interval 5 minute
);