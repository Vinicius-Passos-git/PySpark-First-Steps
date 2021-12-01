# %%
from pyspark.sql import SparkSession
from pyspark import SparkFiles

spark = SparkSession.builder.appName('data_processing').getOrCreate()

url = 'https://raw.githubusercontent.com/Apress/machine-learning-with-pyspark/master/chapter_2_Data_Processing/sample_data.csv'

spark.sparkContext.addFile(url)
df = spark.read.csv(SparkFiles.get("sample_data.csv"), header=True)
# %%
df.columns
# %%
len(df.columns)
# %%
df.count()
# %%
print(df.count(), len(df.columns))
# %%
df.printSchema()
# %%
df.show(5)
# %%
df.select('age', 'mobile')
# %%
df.describe().show()
# %%
#add new columns
df = df.withColumn("age_after_10_yrs", (df["age"]+10))
# %%
df.show()
# %%
#Create a new columns and using Cast to change the datatype
from pyspark.sql.types import StringType,DoubleType
df.withColumn('age_double',df['age'].cast(DoubleType())).show(10,False)
# %%
#Filtering data
df.filter(df['mobile']=='Vivo').show()
# %%
df.filter(df['mobile']=='Vivo').select('age','ratings','mobile').show()
# %%
#Two filter in same script
df.filter(df['mobile']=='Vivo').filter(df['experience'] >10).show()
# %%
df.filter((df['mobile']=='Vivo')&(df['experience'] >10)).show()
# %%
#Distinct value in Column
df.select('mobile').distinct().show()
# %%
df.select('mobile').distinct().count()
# %%
#Grouping data
df.groupBy('mobile').count().show(5,False)
# %%
#Orderby
df.groupBy('mobile').count().orderBy('count',ascending=False).show(5,False)
# %%
#metric by groupby
df.groupBy('mobile').mean().show(5,False)
# %%
df.groupBy('mobile').sum().show(5,False)
# %%
df.groupBy('mobile').max().show(5,False)
# %%
df.groupBy('mobile').min().show(5,False)
# %%
#Aggregations
df.groupBy('mobile').agg({'experience':'sum'}).show(5,False)
# %%
#User-Defined Functions (UDFs)
from pyspark.sql.functions import udf
# %%
def price_range(brand): 
    if brand in ['Samsung','Apple']: return 'High Price'
    elif brand =='MI': return 'Mid Price'
    else: return 'Low Price'

# %%
brand_udf=udf(price_range,StringType())
# %%
df.withColumn('price_range',brand_udf(df['mobile'])).show(10,False)
# %%
# Drop Duplicate Values
df=df.dropDuplicates()
# %%
# Delete Column
df_new=df.drop('mobile')
# %%
df_new.show()
# %%
