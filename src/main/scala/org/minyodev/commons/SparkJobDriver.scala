package org.minyodev.commons

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by aesguerra on 1/12/17
  */
object SparkJobDriver {

  private val LOG = Logger.getLogger(this.getClass().getName())

  private var sconf: SparkConf  = _
  private var sc: SparkContext = _

  val conf: Configuration = new Configuration

  abstract class SparkJob {
    val name: String = "Spark Job Driver"
    val description: String = "loremboyz"
    val version: String = ""

    final val logger = Logger.getLogger(this.getClass().getName())

    def wrappedRun(sc: SparkContext, custArgs:Array[String] = Array()) {
      sc.setJobGroup(this.name, this.description, true)
      this.run(sc, custArgs)
    }

    def run(sc: SparkContext, custArgs: Array[String]): Unit

    def getConf: Configuration = conf
  }

  /**
    * Spark Job Driver's main entry point
    *
    * args(0) main configuration from etc/conf/batch-ingest.xml
    * args(1) implementing class
    *
    */
  def main(args: Array[String]) {
    // parse argument
    if (args.length == 0) {
      LOG.error("SparkJobDriver args(0) args(1)")
      return
    }
    conf.addResource(new Path(args(0)))

    val jobClassName = args(1)
    println("Args: " + args.mkString(","))
    val job = Class.forName(jobClassName).asSubclass(classOf[SparkJob]).newInstance()

    LOG.info("Job Name: " + job.name)
    sconf = new SparkConf().setAppName(job.name)

    // Check for local or clustered
    val setMasterAndExecMem = conf.getBoolean("set.master.and.exec.memory", false)
    if (setMasterAndExecMem) {
      sconf.setMaster("local[4]").set("spark.executor.memory", "4g")
    }

    sc = new SparkContext(sconf)

    var ret = 0

    /**
      * To get the chain going but there's a lot of work to do, we need to give user-space a full control
      * but with a lot of helper tools as well
      */
    try {
      // args.drop(2) shall drop the first 2 args and will start with args[2]
      job.wrappedRun(sc, args.drop(2))
    } catch {
      case e: Exception => {
        LOG.error(e.getMessage)
        e.printStackTrace()
        ret = 1
      }
    }

    // Stop sparkContext
    sc.stop()
    System.exit(ret)
  }
}
