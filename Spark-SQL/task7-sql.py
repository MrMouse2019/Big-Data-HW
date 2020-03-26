import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    # reading into DF
    parking = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])
    # creating SQL temp view from DF
    parking.createOrReplaceTempView("parking")
    # used full outer join to handle cases when one of the average is NULL -- > 0.00
    query = """
with temp1 as(
	select violation_code,
		case
			when day(issue_date) in (5, 6, 12, 13, 19, 20, 26, 27) then "weekend"
			else "weekday"
		end as daytype
	from parking
),
temp2 as(
	select violation_code, daytype, count(*) as cnt
	from temp1
	group by violation_code, daytype
),
temp3 as(
	select violation_code, cnt/8 as weekends_average
	from temp2
	where daytype = "weekend"
),
temp4 as(
	select violation_code, cnt/23 as weekdays_average
	from temp2
	where daytype = "weekday"
)
select 
	case
		when temp3.violation_code is not NULL then temp3.violation_code
		when temp4.violation_code is not NULL then temp4.violation_code
		else NULL
	end as violation_code,
	case
		when weekends_average is NULL then 0.00
		else weekends_average
	end as weekends_average,
	case
		when weekdays_average is NULL then 0.00
		else weekdays_average
	end as weekdays_average
from temp3 full outer join temp4 on (temp3.violation_code = temp4.violation_code)
where temp3.violation_code is not NULL or temp4.violation_code is not NULL
order by violation_code
    """
    result = spark.sql(query)
    # formatting and saving the result
    result.select(format_string('%s\t%.2f, %.2f', result.violation_code, result.weekends_average, result.weekdays_average)).write.save("task7-sql.out", format="text")
