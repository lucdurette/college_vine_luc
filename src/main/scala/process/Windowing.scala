package process

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import util.Helper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql._


object Windowing extends App {
  val spark_conf: Config = ConfigFactory.load("local.conf").getConfig("spark")
  System.setProperty("hadoop.home.dir", "C:\\git\\winutils\\hadoop-3.2.1\\")

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master(spark_conf.getString("mode"))
    .config("spark.sql.warehouse.dir", spark_conf.getString("warehouse_dir"))
    //.config("spark.some.config.option", "some-value")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  //      import spark.implicits._


  import spark.implicits._

  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )
  val df = simpleData.toDF("employee_name", "department", "salary")
  df.show()

  val windowSpec  = Window.partitionBy("department").orderBy(col("salary").desc)
  df.withColumn("row_number",row_number.over(windowSpec))
    .show()
    print("The end")

  val lagDataSchema = StructType(Array(
    StructField("department",StringType,true),
    StructField("money",IntegerType,true),
    StructField("date",DateType,true)
  ))



  val lagData = Seq(("Sales", 3000,"2022-06-20"),
    ("Sales",lit(null).cast("integer"),"2022-06-21"),
    ("Sales", 4100,"2022-06-22"),
    ("Finance", 3000,"2022-06-20"),
    ("Sales", 3000,"2022-06-23"),
    ("Finance", 3300,"2022-06-21"),
    ("Finance", 3900,"2022-06-22"),
    ("Sales", 4100,"2022-06-24")
  )

  val lag_rdd = lagData.toDF()
  lag_rdd.show()

  //val lag_df = spark.createDataFrame(lag_rdd,lagDataSchema)


 // val lag_df = spark.createDataFrame(lagData,lagDataSchema)
//  lag_df.withColumn("add_one", (col("money") + 1) * 2 )
//    .withColumn("when",when(col("money")  <= 3000 || col("money") >=4000, "out_of_range"  ).otherwise("in_range")).show()



  val rando = Seq(("  Sales  "),
    ("Sales"),
    ("223Sales44   "),
    ("  sdsdsd444555gg66  ")
  )

  val clean_df = rando.toDF("mess")
  clean_df.withColumn("cleaned_mess", upper(regexp_replace(col("mess"),"[^a-zA-Z]+","") )).show()
}

