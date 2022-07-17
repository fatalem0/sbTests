package task4.excel

import org.apache.spark.sql.functions.typedlit
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/*
  пример через работу с Excel
  для запуска в конфигурации используем JDK для Java 11
  функционал spark-excel не предусматривает сохранение нескольких листов в один файл,
  для запуска в конфигурации используем SDK для Java 11
 */
object CreateNewTables {
  private val dtf = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")
  private val now = LocalDateTime.now

  /*
    xlsxFilePath - путь до изначальной таблицы
    xlsxNewCallsFilePath - путь до новой таблицы CALLS
    xlsxNewDatesFilePath - путь до новой таблицы DATES
    xlsxStatTotalFilePath - путь до таблицы STAT_TOTAL
    xlsxStatOperFilePath - путь до таблицы STAT_OPER
   */
  private val xlsxFilePath = "src/main/scala/task4/excel/task4_data_before.xlsx"
  private val xlsxNewCallsFilePath = "src/main/scala/task4/excel/task4_data_after/task4_calls.xlsx"
  private val xlsxNewDatesFilePath = "src/main/scala/task4/excel/task4_data_after/task4_dates.xlsx"
  private val xlsxStatTotalFilePath = "src/main/scala/task4/excel/task4_data_after/task4_stat_total.xlsx"
  private val xlsxStatOperFilePath = "src/main/scala/task4/excel/task4_data_after/task4_stat_oper.xlsx"

  /** Метод создает представления таблиц CALLS и DATES
   *
   * @param spark       объект спарк-сессии
   * @param initialPath путь до изначальной таблицы
   */
  def initTables(spark: SparkSession, initialPath: String): Unit = {
    val callsTable = spark.read
      .format("excel")
      .option("header", "true")
      .option("dataAddress", "0!A1:Z65535")
      .load(initialPath)

    callsTable.createOrReplaceTempView("calls")

    val datesTable = spark.read
      .format("excel")
      .option("header", "true")
      .option("dataAddress", "1!A1:Z65535")
      .load(initialPath)

    datesTable.createOrReplaceTempView("dates")
  }

  /** Метод копирует таблицу CALLS по новому пути
   *
   * @param spark       объект спарк-сессии
   * @param initialPath путь до изначальной таблицы
   * @param savePath    путь до копированной таблицы
   */
  def copyCallsTable(spark: SparkSession, initialPath: String, savePath: String): Unit = {
    val callsTable = spark.read
      .format("excel")
      .option("header", "true")
      .option("dataAddress", "0!A1:Z65535")
      .load(initialPath)

    callsTable.write
      .format("excel")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(savePath)
  }

  /** Метод создает таблицу STAT_TOTAL и сохраняет ее
   *
   * @param spark    объект спарк-сессии
   * @param savePath путь сохранения
   */
  def createStatTotalTable(spark: SparkSession, savePath: String): Unit = {
    val countQuery =
      """
        |SELECT calls.date, COUNT(calls.date) as count
        |FROM calls, dates
        |WHERE calls.date = dates.date
        |GROUP BY calls.date
        |ORDER BY calls.date DESC;
        |""".stripMargin

    val countDates = spark.sql(countQuery)
    val finalTable = countDates.withColumn("stat_date_time", typedlit(dtf.format(now)))

    finalTable.write
      .format("excel")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(savePath)
  }

  /** Метод создает таблицы STAT_OPER и сохраняет ее
   *
   * @param spark    объект спарк-сессии
   * @param savePath путь сохранения
   */
  def createStatOperTable(spark: SparkSession, savePath: String): Unit = {
    val countQuery =
      """
        |SELECT calls.date, oper_id, COUNT(*) AS count
        |FROM calls, dates
        |WHERE calls.date = dates.date
        |GROUP BY calls.date, oper_id
        |ORDER BY calls.date DESC, oper_id DESC;
        |""".stripMargin

    val countDatesForOpers = spark.sql(countQuery)
    val finalTable = countDatesForOpers.withColumn("stat_date_time", typedlit(dtf.format(now)))

    finalTable.write
      .format("excel")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(savePath)
  }

  /** Метод очищает таблицу DATES после создания таблиц STAT_TOTAL и STAT_OPER
   *
   * @param spark       объект спарк-сессии
   * @param initialPath путь до изначальной таблицы
   * @param savePath    путь сохранения
   */
  def clearDatesTable(spark: SparkSession, initialPath: String, savePath: String): Unit = {
    val datesTable = spark.read
      .format("excel")
      .option("header", "true")
      .option("dataAddress", "1!A1:Z65535")
      .load(initialPath)

    val schema = datesTable.schema
    val emptyDatesTable = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    emptyDatesTable.write
      .format("excel")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(savePath)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Create New Tables")
      .master("local[*]")
      .getOrCreate()

    initTables(spark, xlsxFilePath)
    copyCallsTable(spark, xlsxFilePath, xlsxNewCallsFilePath)
    createStatTotalTable(spark, xlsxStatTotalFilePath)
    createStatOperTable(spark, xlsxStatOperFilePath)
    clearDatesTable(spark, xlsxFilePath, xlsxNewDatesFilePath)

    spark.close()
  }
}
