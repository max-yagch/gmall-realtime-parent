package app

import java.text.SimpleDateFormat
import java.util.Date

import bean.StartUpLog
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, rdd}
import redis.clients.jedis.Jedis
import util.{KafkaUtil, RedisUtil}
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import yang.common.GmallConstants

object Dau_App {
	def main(args: Array[String]): Unit = {
		// TODO: 上下文环境准备
		val conf: SparkConf = new SparkConf().setAppName("RealtimeStartupApp").setMaster("local[*]")
		//val sc: SparkContext = new SparkContext(conf)
		val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
		val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)


		// TODO: 补充两个字段 date and hour
		val startupLogDstream: DStream[StartUpLog] = inputDStream.map { record =>
			val jsonString: String = record.value()
			val StartUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
			val formatter = new SimpleDateFormat("yyyy-MM-dd HH")
			val datetimeStr: String = formatter.format(new Date(StartUpLog.ts))
			val dateTimeArr: Array[String] = datetimeStr.split(" ")
			StartUpLog.logDate = dateTimeArr(0)
			StartUpLog.logHour = dateTimeArr(1)
			StartUpLog
		}

		// TODO: 本批次内数据根据mid进行过滤 取最早的一个
		startupLogDstream.foreachRDD(rdd => {
			println("过滤前：" + rdd.count())
		})
		val selfDistinctDStream: DStream[StartUpLog] = startupLogDstream.map(t => {
			(t.mid, t)
		}).groupByKey().flatMap(t => {
			t._2.toList.sortWith((Left, Right) => {
				Left.ts < Right.ts
			}).take(1)
		})
		selfDistinctDStream.foreachRDD(rdd => {
			println("本批次内过滤后：" + rdd.count())
		})


		// TODO: redis中查询过滤 把redis中查出来的mid 数据封装进广播变量里

		val redisFilterDStream: DStream[StartUpLog] = selfDistinctDStream.transform { rdd =>
			val client: Jedis = RedisUtil.getJedisClient
			val dateFormater: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
			val dateString: String = dateFormater.format(new Date())
			//redis中的key
			val key: String = "dau:" + dateString

			val midSet: util.Set[String] = client.smembers(key)
			client.close()
			val midBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)
			val redisFilterRDD: RDD[StartUpLog] = rdd.filter(startuplog => !midBC.value.contains(startuplog.mid))
			println("redis过滤后：" + redisFilterRDD.count())
			redisFilterRDD
		}

		redisFilterDStream.cache()

		// TODO: 把过滤后的数据存进redis中辅助实现对数据的去重

		redisFilterDStream.foreachRDD { rdd => {
			rdd.foreachPartition(startuplogitr => {

				val client: Jedis = RedisUtil.getJedisClient
				startuplogitr.foreach(startuplog => {
					val key: String = "dau:" + startuplog.logDate
					client.sadd(key, startuplog.mid)
				})
				client.close()
			}
			)
		}
		}

		redisFilterDStream.foreachRDD{rdd=>
			rdd.saveToPhoenix("GMALLREALTIME0218_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,new Configuration,Some("hadoop102,hadoop103," +
					"hadoop104:2181"))
		}


		ssc.start()
		ssc.awaitTermination()

	}
}
