import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, dense_rank, \
    coalesce, unix_timestamp, round, row_number, desc, lower
from pyspark.sql.window import Window
from dotenv import load_dotenv, dotenv_values

spark = SparkSession \
    .builder \
    .master('local[1]') \
    .appName('spark') \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.5.2.jar") \
    .getOrCreate()

load_dotenv()
creds = dotenv_values(".env")

postgresql_user = os.getenv('POSTGRESQL_USER')
postgresql_password = os.getenv('POSTGRESQL_PASSWORD')
spark_home = os.getenv('SPARK_HOME')
postgres_port = os.getenv('CONTAINER_PORT')

jdbc_connector = spark.read \
    .format('jdbc') \
    .option('driver', 'org.postgresql.Driver') \
    .option('url', f'jdbc:postgresql://localhost:{postgres_port}/postgres') \
    .option('user', postgresql_user) \
    .option('password', postgresql_password)

def load_tables(tables_names: list[str]):
    """Loads tables.
    Input: table_names as list
    Output: dictionary of all tables"""

    tables = {}
    for name in tables_names:
        tables[name] = jdbc_connector.option('dbtable', name).load()
    return tables

tables_names = ['actor', 'film', 'category',
                'film_category', 'film_actor',
                'inventory', 'customer',
                'address', 'city',
                'rental']
tables = load_tables(tables_names)


# 1. Вывести количество фильмов в каждой категории, отсортировать по убыванию:

# SELECT category_id AS category, COUNT(film_id) AS film_count
# FROM film_category
# GROUP BY category_id
# ORDER BY film_count desc;

result1 = tables['film_category'].groupby('category_id')\
        .agg(count('film_id').alias('film_count'))\
        .orderBy(desc('film_count'))
print('res1:\n')
result1.show()


# 2. Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

# SELECT actor.first_name AS fname, actor.last_name AS lname, SUM(film.rental_duration) AS rduration
# FROM actor
#     INNER JOIN film_actor ON actor.actor_id = film_actor.actor_id
#     INNER JOIN film ON film.film_id = film_actor.film_id
# GROUP BY actor.actor_id
# ORDER BY rduration DESC
# LIMIT 10;

result2 = tables['actor'].join(tables['film_actor'], 'actor_id', 'inner')\
        .join(tables['film'], 'film_id', 'inner')\
        .groupby('actor_id')\
        .agg(sum('rental_duration').alias('rduration'))\
        .orderBy(desc('rduration'))\
        .limit(10)
print('res2:\n')
result2.show()


# 3. Вывести категорию фильмов, на которую потратили больше всего денег.

# SELECT category.name AS c_name, SUM(film.rental_rate) AS rate
# FROM category
#     INNER JOIN film_category ON category.category_id = film_category.category_id
#     INNER JOIN film ON film_category.film_id = film.film_id
# GROUP BY category.category_id
# ORDER BY rate DESC
# LIMIT 1;

result3 = tables['category'].join(tables['film_category'], 'category_id', 'inner')\
        .join(tables['film'], 'film_id', 'inner') \
        .groupby('name')\
        .agg(sum('rental_rate').alias('rate'))\
        .orderBy(desc('rate'))\
        .limit(1)
print('res3:\n')
result3.show()


# 4. Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

# SELECT film.film_id, film.title AS f_name
# FROM inventory
#     RIGHT JOIN film ON inventory.film_id = film.film_id
# WHERE inventory.film_id IS NULL;

result4 = tables['inventory'].join(tables['film'], 'film_id', 'right')\
        .where(tables['inventory']['film_id'].isNull())\
        .select('title')
print('res4:\n')
result4.show()


# 5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
# Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.

# SELECT fname,
#        lname,
#        cnt,
#        rank
# FROM (
#     SELECT a.actor_id AS id,
#            a.first_name AS fname,
#            a.last_name AS lname,
#            COUNT(*) AS cnt,
#            RANK() OVER (ORDER BY COUNT(*) DESC) AS rank
#     FROM actor AS a
#         INNER JOIN film_actor AS fa ON a.actor_id = fa.actor_id
#         INNER JOIN film AS f ON f.film_id = fa.film_id
#         INNER JOIN film_category AS fc ON f.film_id = fc.film_id
#         INNER JOIN category AS c ON fc.category_id = c.category_id
# WHERE c.name = 'Children'
# GROUP BY a.actor_id
# ) subquery
# WHERE rank <= 3;

# for check
subquery = tables['actor'].join(tables['film_actor'], 'actor_id', 'inner')\
        .join(tables['film'], 'film_id', 'inner')\
        .join(tables['film_category'], 'film_id', 'inner')\
        .join(tables['category'], 'category_id', 'inner')\
        .where(col('name') == 'Children')\
        .groupby('actor_id', 'first_name', 'last_name')\
        .agg(count("film_id").alias("children_film")) \
        .orderBy(desc("children_film"))
subquery.show()

result_5 = tables['actor'] \
            .join(tables['film_actor'], 'actor_id') \
            .join(tables['film'], 'film_id') \
            .join(tables['film_category'], 'film_id') \
            .join(tables['category'], 'category_id') \
            .filter(col('name') == 'Children') \
            .groupBy('actor_id', 'first_name', 'last_name') \
            .agg(count('actor_id').alias('children_film')) \
            .withColumn('rank', dense_rank().over(Window.orderBy(col('children_film').desc()))) \
            .filter(col('rank') < 3) \
            .orderBy(col('children_film').desc())
print('res5:\n')
result_5.show()


# 6. Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
#    Отсортировать по количеству неактивных клиентов по убыванию.

# SELECT city, customer.active, COUNT(rental.customer_id)
# FROM rental
# 	INNER JOIN customer ON rental.customer_id = customer.customer_id
# 	LEFT JOIN address ON customer.address_id = address.address_id
# 	INNER JOIN city ON address.city_id = city.city_id
# GROUP BY city, customer.active
# ORDER BY customer.active, count DESC;

result6 = tables['rental'].join(tables['customer'], 'customer_id', 'inner')\
        .join(tables['address'], 'address_id', 'left')\
        .join(tables['city'], 'city_id', 'inner')\
        .groupby('city') \
        .agg(sum('active').alias('active'), count('active').alias('all_users'))\
        .withColumn('non_active', col('all_users') - col('active'))\
        .select('city', 'non_active', 'active') \
        .orderBy(col('non_active').desc())
print('res6:\n')
result6.show()


# 7. Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах
#    (customer.address_id в этом city),
#    и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”.
#    Написать все в одном запросе. New way.

rental_time = tables['rental']\
    .withColumn('sum_hours', (coalesce(unix_timestamp('return_date'),
                              unix_timestamp('last_update')) - unix_timestamp('rental_date')) / 3600)\
    .select('rental_id', 'inventory_id', 'customer_id', 'sum_hours')\
    .orderBy(col('sum_hours').desc())

result7 = tables['category']\
        .join(tables['film_category'], 'category_id', 'inner')\
        .join(tables['film'], 'film_id', 'inner')\
        .join(tables['inventory'], 'film_id', 'inner')\
        .join(rental_time, 'inventory_id', 'inner')\
        .join(tables['customer'], 'customer_id', 'inner')\
        .join(tables['address'], 'address_id', 'inner')\
        .join(tables['city'], 'city_id', 'inner')\
        .filter(col('city').like('A%') | col('city').like(r'%-%'))\
        .groupBy('city_id', 'city', 'category_id', 'name')\
        .agg(round(sum('sum_hours'), 2).alias('sum_hours'))\
        .withColumn('rank', row_number().over(Window.partitionBy(col('city_id')).orderBy(col('sum_hours').desc())))\
        .filter(col('rank') == 1)\
        .limit(1)
print('res7:\n')
result7.show()