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
select plate_id, registration_state, count(*) as total_number
from parking
group by plate_id, registration_state
order by total_number desc, plate_id
limit 20
    """
    result = spark.sql(query)
    # formatting and saving the result
    result.select(format_string('%s, %s\t%d', result.plate_id, result.registration_state, result.total_number)).write.save("task6-sql.out", format = "text")
