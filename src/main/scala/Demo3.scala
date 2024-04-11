import org.apache.spark.sql.SparkSession

object Demo3 extends App {
  val spark = SparkSession
    .builder()
    .config("spark.serializer", value="org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .config("spark.sql.caseSensitive", value = true)
    .config("spark.sql.session.timeZone", value = "UTC")
    .config("spark.driver.memory", value = "1G")
    .appName("my-test")
    .master("local[*]")
    .getOrCreate()

  spark.sql("CREATE DATABASE hudidb")
  spark.sql("USE hudidb")
  val location = java.nio.file.Files.createTempDirectory("trick-test").toAbsolutePath.toString

  spark.sql(s"""
           create table trick(
            tricky struct<tricky decimal(10,2)>
            )
            using hudi
            location '$location'
           """)

  spark.sql("""
             insert into trick
             values (
               named_struct('tricky', 1.2)
             )
             """) // works fine

  spark.sql("""
             insert into trick
             values (
               named_struct('tricky', 3.4)
             )
             """) // org.apache.avro.SchemaParseException: Can't redefine: tricky
}
