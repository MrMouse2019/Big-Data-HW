import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    # reading into DF
    open = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
    # creating SQL temp view from DF
    open.createOrReplaceTempView("open")
    query = """
select license_type, sum(amount_due) as total, avg(amount_due) as average
from open
group by license_type
order by license_type
    """
    result = spark.sql(query)
    # formatting and saving the result
    result.select(format_string('%s\t%.2f, %.2f', result.license_type, result.total, result.average)).write.save("task3-sql.out", format= "text")
