# Jamel Peralta Coss
# 802-13-5870

# This is for creating spark session for sparkSql
spark = SparkSession.builder.getOrCreate()

# Getting the values from the csv to a data frame 1 and 2
dfSchools = spark.read.format("csv").load("/home/jhony_huallparimachi/spark/escuelasPR.csv")
dfStudents = spark.read.format("csv").load("/home/jhony_huallparimachi/spark/studentsPR.csv")

# Setting the data frame column names such that is better to understand
# and for later be able to use join
dfSchools = dfSchools.toDF("region","distrito","ciudad","eid","enombre","nivel","college")
dfStudents = dfStudents.toDF("region","distrito","eid","enombre","nivel","sexo","sid")

# Exercise 1
dfSchools.join(dfStudents,'eid').filter(dfSchools.nivel == "Superior").filter(dfStudents.sexo == "M").filter((dfSchools.ciudad == "Ponce") | (dfSchools.ciudad == "San Juan")).show()

# Exercise 2
dfSchools.filter(dfSchools.region == "Arecibo").groupBy(dfSchools.distrito, dfSchools.ciudad).count().show()
