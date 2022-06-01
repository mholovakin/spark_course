import pyspark.sql.functions as f
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

conf = SparkConf().setAppName("OfficeDataProject")
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

df = spark.read.options(header=True, inferSchema=True).csv('file:///spark/datasets/OfficeDataProject.csv')

total_employees = df.select(df.employee_id).distinct().count()  # --> total nums of employees

total_departments = df.select(df.department).distinct().count()  # --> total nums of departments

departments = df.select(df.department).distinct()  # --> departments in company

total_employees_per_department = df.groupby(df.department).agg(f.count('employee_name'))  # --> tot employee per dept

total_employees_per_state = df.groupby(df.state).agg(f.count('employee_name'))  # --> tot employee per state

per_state_per_dept = df.groupby(df.department, df.state).agg(f.count('employee_name'))  # --> emp per state per dept

min_max_salary = df.groupby(df.department).agg(f.min('salary').alias('min_salary'), f.max('salary').alias('max_salary'))
min_max_salary = min_max_salary.orderBy(min_max_salary.min_salary.asc(),
                                        min_max_salary.max_salary.asc())  # --> in asc order


'''Print the names of employees working in NY state under Finance department whose bonuses are greater than the avg 
bonuses of employees in NY state '''
avg_bonus = df.filter(df.state == 'NY').groupby('state').agg(f.avg('bonus').alias('avg_bonus')).collect()[0][
    'avg_bonus']
employees = df.select(df.state, 'employee_name', df.bonus).filter((df.state == 'NY') & (df.bonus > avg_bonus))


'''Raise the salaries $500 of all employees whose age is greater than 45'''


def raise_salary(age, salary):
    if age > 45:
        return salary + 500
    else:
        return salary


raiseUDF = f.udf(lambda x, y: raise_salary(x, y), IntegerType())

raised_salaries = df.withColumn('salary', raiseUDF(df.age, df.salary))
