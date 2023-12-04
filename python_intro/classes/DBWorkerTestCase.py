import unittest

from classes.DBWorker import DBWorker


class DBWorkerTestCase(unittest.TestCase):
    """This class has some unittests for ConnectionDB class.
    There is one class method get_query_from_file"""

    def test_get_query_from_file(self):
        """Check query create_index.sql
        input: string of SQL commands
        return: true or false, equal or not"""

        self.assertEqual(
            DBWorker.get_query_from_file("pg_queries/create_index.sql"),
            "create index id_room_index on students(id_room);",
        )

    def test_get_query_from_file2(self):
        """Check query create_rooms.sql
         input: string of SQL commands
         return: true or false, equal or not"""

        self.assertEqual(
            DBWorker.get_query_from_file("pg_queries/create_rooms.sql"),
            """CREATE TABLE IF NOT EXISTS rooms(
	id SERIAL NOT NULL PRIMARY KEY
);""",
        )

    def test_get_query_from_file3(self):
        """Check query create_students.sql
         input: string of SQL commands
         return: true or false, equal or not"""

        self.assertEqual(
            DBWorker.get_query_from_file("pg_queries/create_students.sql"),
            """CREATE TABLE IF NOT EXISTS students(
	id SERIAL NOT NULL PRIMARY KEY,
	id_room SERIAL NOT NULL REFERENCES rooms(id),
	age INTEGER NOT NULL,
	gender VARCHAR(6) NOT NULL
);""",
        )


if __name__ == "__main__":
    unittest.main()
