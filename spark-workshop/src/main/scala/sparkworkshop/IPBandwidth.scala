// package com.foo.bar    // You could put the code in a package...

import com.typesafe.sparkworkshop.util.FileUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

// Implicit conversions, such as methods defined in
// org.apache.spark.rdd.PairRDDFunctions
// (http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions)
import org.apache.spark.SparkContext._



object IPBandwith {
  def main(args: Array[String]): Unit = {

    // The first argument specifies the "master" (see the tutorial notes).
    // The second argument is a name for the job. Additional arguments
    // are optional.
    val sc = new SparkContext("local", "IP Bandwidth")
    val sqlc = new SQLContext(sc)
    import sqlc._


    try {

      val out = "output/kjv-wc2"


      // val lineLog = """^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\s(-\s-)\s(\[.*\])\s(".*")\s([0-9]{3})\s([0-9]*)\s(.*)$""".r
      val lineLog = """^(.*)\s-\s-\s\[.*\]\s".*"\s([0-9]{3})\s(\d+)\s(.*)$""".r
      val timestart = System.currentTimeMillis()
      val input = sc.textFile("data/apache") collect {
        case lineLog(a,_,o,_) =>
          (a,o.toLong)

      }

      input.cache

      val ips = input
        .reduceByKey((count1, count2) => count1 + count2).sortBy(z => -z._2)

      val timeIntermediate = System.currentTimeMillis() - timestart
      ips.take(10).foreach(println)
      ips.filter(z => z._1 == "213.175.86.38").foreach(println)
      println(s"Final time: $timeIntermediate")
      println(s"Writing output to: $out")
      ips.saveAsTextFile(out)
/*
      println(s"Intermediate time: $timeIntermediate")

      wc.registerTempTable("kjv_ip")
      wc.cache()

      val topWCs = sql("""
            SELECT * FROM kjv_ip
            ORDER BY _2 DESC LIMIT 10""")
      topWCs.saveAsTextFile(s"out-top10")
      val timeFinal = System.currentTimeMillis() - timestart
      println(s"Final time: $timeFinal")
     // wc.take(12).foreach(println)
*/
    } finally {
      sc.stop()      // Stop (shut down) the context.
    }


  }
}
