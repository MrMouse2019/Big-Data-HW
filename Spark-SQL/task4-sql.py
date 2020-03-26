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
select registration_state, sum(total_number) as total_number
from(
	select case registration_state 
			when 'NY' then 'NY'
			else 'Other'
		end as registration_state, count(*) as total_number
	from parking
	group by registration_state
)
group by registration_state
order by registration_state
    """
    result = spark.sql(query)
    # formatting and saving the result
    result.select(format_string('%s\t%d', result.registration_state, result.total_number)).write.save("task4-sql.out", format = "text")

