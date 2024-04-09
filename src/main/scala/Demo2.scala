import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Collections

object Demo2 extends App {
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

  val location = java.nio.file.Files.createTempDirectory("trick-test").toAbsolutePath.toString

  class Inner {
    var getTrick: java.math.BigDecimal = java.math.BigDecimal.TEN // <- Same fieldname, different type
  }

  class Trick {
    var getHello: String = "hello"
  }

  class Proba {
    var getTs: Int = 1
    var getTrick: Trick = new Trick() // <- Same fieldname, different type
    var getInner: Inner = new Inner()
  }

  val proba: Proba = new Proba()
  val df = spark.createDataFrame(Collections.singletonList(proba), proba.getClass)

  val hudiOptions: Map[String, String] = Map(
    "hoodie.database.name" -> "default",
    "hoodie.table.name" -> "trick",
    "hoodie.datasource.write.storage.type" -> "COPY_ON_WRITE", // Default value, added for readability
    "hoodie.datasource.write.operation" -> "upsert", // Default value, added for readability
    "hoodie.datasource.write.hive_style_partitioning" -> "true",
  )

  df.show(100, false)
  df.write
    .format("hudi")
    .options(hudiOptions)
    .mode(SaveMode.Append)
    .save(location)

  val proba2: Proba = new Proba()

  val df2 = spark.createDataFrame(Collections.singletonList(proba2), proba2.getClass)
  df2.show(100, false)
  df2.write
    .format("hudi")
    .options(hudiOptions)
    .mode(SaveMode.Append)
    .save(location) // org.apache.avro.SchemaParseException: Can't redefine: trick

  // spark.newSession().read.format("hudi").load(location).show(100, false)
}
