package process

import com.typesafe.config.{Config, ConfigFactory}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import util.Helper


object Ingestion extends App {
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

  val file_to_process = Helper.getListOfFiles("dw/raw_data")
    .map(_.toString)
  println(file_to_process)

  for (path <- file_to_process) {
    val stripped_path = Helper.stripPath(path).split('.')
    val filetype = stripped_path(1)
    val source = stripped_path(0)

    filetype match {
      case "csv" => print("I am in csv")
      case "json" => {
        val load_df = spark.read.format("json")
          .option("multiLine", true).option("mode", "PERMISSIVE")
          .load(path)
        load_df.show()
      }

    }
  }
    //load_df.show()
    //      val jdbcDF = spark.read.json()
    //        .format("jdbc")
    //        .option("url", "postgres://admin:admin@localhost:5455/postgres")
    //        .option("dbtable", "schema.tablename")
    //        //.option("user", "username")
    //        //.option("password", "password")
    //        .load()

    print("The end")
  }

