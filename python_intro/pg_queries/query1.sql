-- Список комнат и количество студентов в каждой из них
SELECT rooms.id as room, COUNT(students.id) AS stud_count FROM
students INNER JOIN rooms ON students.id_room = rooms.id
GROUP BY rooms.id
ORDER BY rooms.id;
