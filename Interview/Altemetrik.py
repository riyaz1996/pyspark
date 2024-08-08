import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Starting Spark application")

spark = SparkSession.builder.getOrCreate()
logger.info("Spark session created")

# Creating schema for emp_sal
emp_sal_schema = StructType(
    [
        StructField("ID", IntegerType(), False),
        StructField("EmpID", StringType(), False),
        StructField("Name", StringType(), False),
        StructField("Salary_Date", StringType(), False),
        StructField("salary", IntegerType(), False)
    ]
)

# Loading emp_sal from dbfs
logger.info("Loading emp_sal from CSV")
df_emp_sal = spark.read.csv("emp_sal.csv", sep="~", schema=emp_sal_schema, header=True)
logger.info("emp_sal DataFrame loaded")

# Creating temp view to format date column in emp_sal
df_emp_sal.createOrReplaceTempView('emp_sal')
df_emp_sal_cleansed = spark.sql("SELECT ID, EmpID, Name, to_date(Salary_Date, 'd/M/yyyy') as Salary_Date, salary FROM emp_sal")
logger.info("Formatted Salary_Date in emp_sal")

# Loading emp_info from CSV
logger.info("Loading emp_info from CSV")
df_emp_info = spark.read.csv("emp_info.csv", sep=",", inferSchema=True, header=True)
logger.info("emp_info DataFrame loaded")

# Cleaning and formatting date columns in emp_info
df_with_clean_date = df_emp_info.withColumn("update_at_clean", regexp_replace("update_at", "(\\d{1,2})(st|nd|rd|th)", "$1")) \
                                .withColumn("update_at", to_date("update_at_clean", "d MMM yyyy"))

df_with_clean_date.createOrReplaceTempView('emp_info')
df_emp_info_cleansed = spark.sql("""
    SELECT
        ID, Emp_id, to_date(DOB, 'd/M/yyyy') as DOB,
        Designation, update_at as start_date,
        lead(update_at) OVER (PARTITION BY Emp_id ORDER BY update_at) as end_date,
        CASE
            WHEN lead(update_at) OVER (PARTITION BY Emp_id ORDER BY update_at) IS NULL THEN 1
            ELSE 0
        END as Is_Active
    FROM emp_info
""")

df_emp_info_cleansed = df_emp_info_cleansed.withColumn("age", F.floor(F.datediff(F.current_timestamp(), F.col("DOB")) / 365))
logger.info("Cleaned and formatted emp_info")

# Creating temp views
df_emp_info_cleansed.createOrReplaceTempView('df_emp_info_cleansed')
df_emp_sal_cleansed.createOrReplaceTempView('df_emp_sal_cleansed')
logger.info("Created temp views for cleansed DataFrames")

# Join and filter operations
emp_sal_detail = spark.sql("""
    SELECT a.ID, a.EmpID, a.Name, a.Salary_Date, a.salary, b.Designation, b.age
    FROM df_emp_sal_cleansed a
    LEFT JOIN df_emp_info_cleansed b ON a.ID = b.ID AND b.Is_Active = 1
    WHERE b.age >= 30 AND b.age < 50
""")
logger.info("Joined emp_sal and emp_info data")

# Calculate latest salary increments
df_latest_salary = spark.sql("""
    SELECT *
    FROM (
        SELECT *,
            ((salary - lag(salary) OVER (PARTITION BY EmpID ORDER BY Salary_Date)) / salary) * 100 as Increment_Percent,
            ROW_NUMBER() OVER (PARTITION BY EmpID ORDER BY Salary_Date DESC) as RN
        FROM df_emp_sal_cleansed
    ) a
    WHERE RN = 1
""")
logger.info("Calculated latest salary increments")

# Write the result to Parquet with partitioning
df_latest_salary.write.partitionBy('EmpID').parquet('Final')
logger.info("Written final DataFrame to Parquet with partitioning by EmpID")

logger.info("Spark application finished")
