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
select violation_code, count(*) as cnt
from parking
group by violation_code
order by violation_code
    """
    result = spark.sql(query)
    # formatting and saving the result
    result.select(format_string('%d\t%d', result.violation_code, result.cnt)).write.save("task2-sql.out", format = "text")
    