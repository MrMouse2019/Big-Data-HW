import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, date_format

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    # reading into DF
    parkingDF = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
    openDF = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[2])
    # creating SQL temp view from DF
    parkingDF.createOrReplaceTempView("parking")
    openDF.createOrReplaceTempView("open")
    # using subtract to get (parking - open)
    parkingDF.select('summons_number').subtract(openDF.select('summons_number')).createOrReplaceTempView("temp1")
    query = """
select parking.summons_number, plate_id, violation_precinct, violation_code, issue_date
from parking join temp1 using(summons_number)
order by parking.summons_number
    """
    result = spark.sql(query)
    # formatting and saving the result
    result.select(format_string('%d\t%s, %d, %d, %s', result.summons_number, result.plate_id, result.violation_precinct, result.violation_code, date_format(result.issue_date, 'yyyy-MM-dd'))).write.save("task1-sql.out", format = "text")
