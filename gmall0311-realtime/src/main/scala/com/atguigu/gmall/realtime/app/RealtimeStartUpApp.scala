package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.constant.GmallConstants
import com.atguigu.gmall.realtime.bean.StartUpLog
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

object RealtimeStartUpApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall2019")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val startupStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    val startUpLogDStream: DStream[StartUpLog] = startupStream.map {
      record => {
        val jsonStr: String = record.value()
        val start: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])
        val dateTimeStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(start.ts))
        val dateArr: Array[String] = dateTimeStr.split(" ")
        start.logDate = dateArr(0)
        start.logHour = dateArr(1)
        start
      }
    }
    //多线程错误（有数据残留）
    startUpLogDStream.cache()

    //批次间过滤
    val filteredDStream: DStream[StartUpLog] = startUpLogDStream.transform {
      rdd =>
        val jedis: Jedis = RedisUtil.getJedisClient
        //driver端按周期执行
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        val key = "dau:" + dateStr
        println("=================" + key)
        val dauMidSet: util.Set[String] = jedis.smembers(key)
        jedis.close()

        val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)
        println("过滤前：" + rdd.count())
        val filterRDD: RDD[StartUpLog] = rdd.filter {
          //Executor
          startUpLog => {
            !dauMidBC.value.contains(startUpLog.mid)
          }
        }
        println("过滤后：" + filterRDD.count())
        filterRDD
    }

    //批次内去重：按照mid进行分组，每组取第一个值
    val groupByMidDStream: DStream[(String, Iterable[StartUpLog])] = filteredDStream.map(startUpLog => (startUpLog.mid, startUpLog)).groupByKey()
    val distinctDStream: DStream[StartUpLog] = groupByMidDStream.flatMap {
      case (mid, iter) => {
        iter.toList.take(1)
      }
    }

    //保存今日访问过的用户mid清单:Redis 1.key类型：set 2.key:dau:date(yyyy-MM-dd) 3.value：Set<mid>
    distinctDStream.foreachRDD {rdd =>
        rdd.foreachPartition {startUpLogItr =>
            val jedis: Jedis = RedisUtil.getJedisClient
            for (startUpLog <- startUpLogItr) {
              val key = "dau:" + startUpLog.logDate
              jedis.sadd(key, startUpLog.mid)
              println(startUpLog)
            }
            jedis.close()
        }
    }

    distinctDStream.foreachRDD {rdd =>
      //把数据写入hbase + phoenix
      rdd.saveToPhoenix("GMALL2019_DAU", Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration, Some("hadoop113,hadoop114,hadoop115:2181"));
    }
    println("启动")
    ssc.start()
    ssc.awaitTermination()

  }
}
