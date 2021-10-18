def getMean(arr:Iterable[Double]): Double = {
var count = arr.count(_=> true);
var total = arr.reduce( (x,y) => x+y  );
var mean = total/count;
mean
};

def getVariance(arr:Iterable[Double]): Double = {
var count = arr.count(_ => true);
var linMean = getMean(arr);
var total = arr.map(x=>(x-linMean)*(x-linMean)).reduce( (x,y) => x+y  );
var sqMean = total.toDouble/count.toDouble;
sqMean; 
};

val sqlContext= new org.apache.spark.sql.SQLContext(sc);
import sqlContext._;
import sqlContext.implicits._;
import org.apache.spark.sql.functions._;

//Importing and preparing data
val affairsData=sc.textFile("Affairs.csv");
val headerAndRows = affairsData.map(line => line.split(",").map(_.trim));
val header = headerAndRows.first;
val body = headerAndRows.filter(_(0) != header(0));
val population=body.map(x=>(x(2),x(1).toDouble)).cache();
//population.foreach(println);
val pop=population.toDF("gender","affairs");
val resultPopulation = pop.groupBy("gender").agg(mean("affairs"), variance("affairs")).sort($"gender").select(col("gender").as("Category"),col("avg(affairs)").as("Mean"),col("var_samp(affairs)").as("Variance"));

// Equal samples by categorical value
var males = population.filter(x=>x._1.contains("\"male\""))
var females = population.filter(x=>x._1.contains("\"female\""))
males = males.sample(false,0.25)
females = females.sample(false,0.25)
val popSample= males.union(females)
//popSample.foreach(println);
var totalElems = 0;
var finalDf=popSample.sample(true,1).groupByKey().map(x=>(x._1,(getMean(x._2),getVariance(x._2)))).map(a => (a._1, a._2._1, a._2._2)).toDF("Category", "Mean", "Variance");


// Iterations
for( i <- 1 to 10 ){
	var rl = List[org.apache.spark.rdd.RDD[(String, (Double, Double))]]();

	(1 to 100).foreach(_ => {var resampleData=popSample.sample(true,1).groupByKey().map(x=>(x._1,(getMean(x._2),getVariance(x._2)))); rl = resampleData :: rl; } );
	totalElems = totalElems + rl.length

	val arr = rl.reduce(_ union _).map(a => (a._1, a._2._1, a._2._2));
	val newDf = arr.toDF("Category", "Mean", "Variance");

	var tempDf = finalDf.unionAll(newDf);
	finalDf = tempDf;
	println("Iteration: "+i+" Total Samples: " + totalElems);
   }


println("Population results:");
resultPopulation.show();


println("Result of the sample after bootstraping");
val resultBoot = finalDf.groupBy("Category").agg(mean("Mean"), mean("Variance")).sort($"Category");
resultBoot.show();
