import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.linalg.Vectors

object LogAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogAnalysis")
    val sc = new SparkContext(conf)

    // Load log data
    val data = sc.textFile("test1/FPT-2018-12-02.log")

    // Function to filter records
    def filterRecords(line: String): Boolean = {
      val fields = line.split(" ")
      val criteria = fields.length == 7 &&
        fields(0).toDouble >= 0 &&
        fields(6).forall(Character.isDigit) &&
        fields(6).toInt > 0 &&
        fields(2) != "-"
      criteria
    }

    // Filter log data
    val filterData = data.filter(filterRecords)

    // Function to classify service
    def classifyService(line: String): String = {
      val contentName = line.split(" ")(5)
      if (contentName.endsWith(".mpd") || contentName.endsWith(".m3u8")) {
        "HLS"
      } else if (contentName.endsWith(".dash") || contentName.endsWith(".ts")) {
        "MPEG-DASH"
      } else {
        "Web Service"
      }
    }

    // Classify and count service groups
    val filteredAndClassifiedData = filterData.map(line => (classifyService(line), 1))
    val serviceGroupCounts = filteredAndClassifiedData.reduceByKey(_ + _)
    serviceGroupCounts.collect().foreach { case (serviceGroup, count) =>
      println(s"$serviceGroup: $count records")
    }

    // Function to extract IP
    def extractIP(line: String): String = {
      val fields = line.split(" ")(1)
      fields
    }

    // Get unique IPs
    val uniqueIPs = filterData.map(extractIP).distinct()

    // Load IP information
    val ipInfoData = sc.textFile("test1/IPDict.csv")
    val ipInfoMap = ipInfoData.map(line => {
      val fields = line.split(",")
      (fields(0), (fields(1), fields(2), fields(3)))
    }).collectAsMap()
    val ipInfoBroadcast = sc.broadcast(ipInfoMap)

    // Function to enrich log record
    def enrichLogRecord(line: String): (String, (String, String, String), String, Double, String, Long) = {
      val fields = line.split(" ")
      val ip = fields(1)
      val additionalInfo = ipInfoBroadcast.value.getOrElse(ip, ("Unknown", "Unknown", "Unknown"))
      val latency = fields(0).toDouble
      val city = additionalInfo._2
      val contentSize = fields(fields.length - 1).toLong
      (ip, additionalInfo, city, latency, fields(5), contentSize)
    }

    // Enrich logs with additional information
    val enrichedLogs = filterData.map(enrichLogRecord)

    // Get unique ISPs
    val uniqueISPs = enrichedLogs.map { case (_, (_, _, isp), _, _, _, _) => isp }.distinct().collect()
    println(s"Number of unique ISPs: ${uniqueISPs.length}")

    // Get records from Ho Chi Minh City
    val hcmRecords = enrichedLogs.filter { case (_, (_, city, _), _, _, _, _) => city == "Ho Chi Minh City" }
    println(s"Number of records from Ho Chi Minh City: ${hcmRecords.count()}")

    // Get traffic from Hanoi
    val hanoiTraffic = enrichedLogs.filter { case (_, (_, city, _), _, _, _, _) => city == "Hanoi" }
      .map { case (_, _, _, _, _, contentSize) => contentSize }.reduce(_ + _)
    println(s"Total traffic from Hanoi: $hanoiTraffic")

    // Get latency statistics
    val latencies = enrichedLogs.map { case (_, _, _, latency, _, _) => latency }
    val latenciesVector = latencies.map(latency => Vectors.dense(latency))
    val latencyStats: MultivariateStatisticalSummary = Statistics.colStats(latenciesVector)
    println(s"Mean Latency: ${latencyStats.mean(0)}")
    println(s"Maximum Latency: ${latencyStats.max(0)}")
    println(s"Minimum Latency: ${latencyStats.min(0)}")

    // Additional logic for median, maximum, and second maximum latency
    def getLatency(line: Double): Double = line
    val sortedLate = latencies.sortBy(getLatency)
    val median = (sortedLate.count() + 1) / 2 - 1
    val medianValue = sortedLate.collect()(median.toInt)

    println(s"Median Latency: $medianValue")

    // For checking if the list is sorted
    val maximumValue = sortedLate.collect()(sortedLate.count().toInt - 1)
    println(s"Maximum Latency (sorted list): $maximumValue")

    // Second Max
    val secondMax = sortedLate.collect()(sortedLate.count().toInt - 2)
    println(s"Second Maximum Latency: $secondMax")

    sc.stop()
  }
}
