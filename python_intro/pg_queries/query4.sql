-- Список комнат где живут разнополые студенты
SELECT rooms.id AS room FROM
students INNER JOIN rooms
ON students.id_room = rooms.id
GROUP BY rooms.id
HAVING COUNT(DISTINCT students.gender) = 2;
