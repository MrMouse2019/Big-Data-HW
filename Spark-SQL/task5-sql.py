import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    # reading into DF
    parking = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
    # creating SQL temp view from DF
    parking.createOrReplaceTempView("parking")
    query = """
with temp1 as(
	select plate_id, registration_state, count(*) as total_number
	from parking
	group by plate_id, registration_state
),
temp2 as(
	select max(total_number) as max_num
	from temp1
	)
select plate_id, registration_state, total_number
from temp1, temp2
where temp1.total_number = temp2.max_num
    """
    result = spark.sql(query)
    # formatting and saving the result
    result.select(format_string('%s, %s\t%d', result.plate_id, result.registration_state, result.total_number)).write.save("task5-sql.out", format = "text")
