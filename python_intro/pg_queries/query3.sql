-- 5 комнат с самой большой разницей в возрасте студентов
SELECT rooms.id AS room,
	MAX(students.age) - MIN(students.age) AS max_diff_age
FROM students INNER JOIN rooms 
ON students.id_room = rooms.id
GROUP BY rooms.id
ORDER BY max_diff_age DESC
LIMIT 5;
