val rddFromFile = spark.sparkContext.textFile("Affairs.csv")

val rdd = rddFromFile.map(f=>{
    f.split(",")
  })

rdd.foreach(f=>{
    println("Affairs:"+f(0)+",Gender:"+f(1))
  })  