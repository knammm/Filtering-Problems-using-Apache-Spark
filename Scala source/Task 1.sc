import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.TimeZone

object LogProcessing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Log Processing")
      .getOrCreate()

    val data = spark.sparkContext.textFile("test1/FPT-2018-12-02.txt")

    // Function to filter correct records
    def filterRecords(line: String): Boolean = {
      val fields = line.split(" ")
      val criteria = fields.length == 7 &&
        fields(0).toDouble >= 0 &&
        fields(6).forall(Character.isDigit) &&
        fields(6).toInt > 0 &&
        fields(2) != "-"

      criteria
    }

    // Function to convert time format
    def convert(line: String): Long = {
      val fields = line.split(" ")
      val inputData = fields(3) + " " + fields(4)
      val timeFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]")
      timeFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
      val timeStamp = timeFormat.parse(inputData).getTime()

      timeStamp
    }

    // Filter correct records
    val filterData = data.filter(filterRecords)

    // Sort by converted timestamp
    val sortedData = filterData.sortBy(convert)

    // Displaying results
    println("Top 10 correct records:")
    sortedData.take(10).foreach(println)

    // Function to filter incorrect records
    def filterRecordsFail(line: String): Boolean = !filterRecords(line)

    // Filter incorrect records
    val filterFail = data.filter(filterRecordsFail)

    // Sort incorrect records by converted timestamp
    val sortedFail = filterFail.sortBy(convert)

    // Displaying results
    println("\nTop 10 incorrect records:")
    sortedFail.take(10).foreach(println)

    spark.stop()
  }
}