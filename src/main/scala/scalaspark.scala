import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object scalaspark
{

  def main(args: Array[String]) =
  {

    val logger = Logger.getLogger(getClass.getName)


    val conf: SparkConf = new SparkConf().setAppName("Spark").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark: SparkSession = SparkSession.builder().getOrCreate()
    import spark.implicits._


    val df = spark.read.format("csv")
             .option("header", value = true)
             .option("inferSchema", value = true)
              .option("path", "D:///Data/Datasets/usdata1")
              .load()
    df.show()

  }
}


