from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, sum, when, col

"""CONFIGURATION"""
spark = SparkSession.builder.config("spark.jars", "/usr/local/postgresql-42.4.0.jar") \
    .master("local").appName("task").getOrCreate()

df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/pagila") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "postgres") \
    .option("password", "25102020")


"""TABLES"""
category = df.option("dbtable", "category").load()
film_category = df.option("dbtable", "film_category").load()
rental = df.option("dbtable", "rental").load()
film_actor = df.option("dbtable", "film_actor").load()
actor = df.option("dbtable", "actor").load()
inventory = df.option("dbtable", "inventory").load()
payment = df.option("dbtable", "payment").load()
film = df.option("dbtable", "film").load()
customer = df.option("dbtable", "customer").load()
address = df.option("dbtable", "address").load()
city = df.option("dbtable", "city").load()
country = df.option("dbtable", "country").load()


"""TASK 1"""
join = film_category\
    .join(category, category.category_id == film_category.category_id, "inner")\
    .groupBy(['name'])\
    .count()
print(join.show())


"""TASK 2"""
join = rental\
        .join(inventory, rental.inventory_id == inventory.inventory_id)\
        .join(film_actor, inventory.film_id == film_actor.film_id)\
        .join(actor, film_actor.actor_id == actor.actor_id)
concat_data = join.select(concat_ws(' ', join.first_name, join.last_name).alias('name'))
group_data = concat_data.groupBy(['name']).count()
result = group_data.orderBy(group_data['count'].desc()).limit(10)

print(result.show())


"""TASK 3"""
join = category\
        .join(film_category, category.category_id == film_category.category_id)\
        .join(inventory, film_category.film_id == inventory.film_id)\
        .join(rental, inventory.inventory_id == rental.inventory_id)\
        .join(payment, rental.rental_id == payment.rental_id)

group_data = join.groupBy(['name']).agg(sum('amount').alias('sum'))
result = group_data.orderBy(group_data['sum'].desc())
limit_result = result.limit(1)
print(limit_result.show())


"""TASK 4"""
distinct = inventory.select(inventory.film_id).distinct()
join = film\
    .join(distinct, film.film_id == distinct.film_id, how='full')\
    .filter(distinct.film_id.isNull())\
    .select(film.title, film.film_id)
print(join.show())


"""TASK 5"""
join = category\
        .join(film_category, category.category_id == film_category.category_id)\
        .join(film_actor, film_category.film_id == film_actor.film_id)\
        .join(actor, film_actor.actor_id == actor.actor_id)
where = join.filter(join.name == 'Children')
concat_data = where.select(concat_ws(' ', join.first_name, join.last_name).alias('name_full'), film_category.film_id)
group_data = concat_data.groupBy(concat_data.name_full).count()
result = group_data.orderBy(group_data['count'].desc()).limit(3)
print(result.show())


"""TASK 6"""
join = customer \
    .join(address, customer.address_id == address.address_id) \
    .join(city, city.city_id == address.city_id) \
    .join(country, country.country_id == city.country_id) \
    .withColumn('yes', when(customer.active == 1, 1).otherwise(0)) \
    .withColumn('no', when(customer.active == 0, 1).otherwise(0)) \
    .groupBy(['country']) \
    .agg({'yes': 'sum',
          'no': 'sum'})
print(join.orderBy(join['sum(no)'].desc()).show())


"""TASK 7"""
join = rental \
    .join(inventory, inventory.inventory_id == rental.inventory_id) \
    .join(film, film.film_id == inventory.film_id) \
    .join(customer, rental.customer_id == customer.customer_id) \
    .join(film_category, film.film_id == film_category.film_id) \
    .join(category, film_category.category_id == category.category_id) \
    .join(address, customer.address_id == address.address_id) \
    .join(city, city.city_id == address.city_id) \
    .filter(col("city").like("a%") & (col("city").like('%-%'))) \
    .groupBy(['name', 'city']) \
    .agg({'rental_duration': 'sum'}) \

print(join.orderBy(join['sum(rental_duration)'].desc()).limit(1).show())
