package app

import bean.{AlertInfo, EventInfo}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{KafkaUtil, EsUtil}
import java.util


import scala.util.control.Breaks._
import yang.common.GmallConstants


object AlertApp {
	def main(args: Array[String]): Unit = {

		val conf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")
		val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))


		val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
		// 0 转换为 样例类
		val eventInfoDStream: DStream[EventInfo] = inputDStream.map { record =>
			val jsonString: String = record.value()
			val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])

			eventInfo


		}


		//    1  同一设备  ---> 分组  groupby
		val groupByMidEventInfoDStream: DStream[(String, Iterable[EventInfo])] = eventInfoDStream.map(eventinfo => (eventinfo.mid, eventinfo)).groupByKey()

		//    2  5分钟内 ( 课堂演示统计30秒)    滑动窗口  窗口大小  滑动步长
		val windowDStream: DStream[(String, Iterable[EventInfo])] = groupByMidEventInfoDStream.window(Seconds(30), Seconds(5))

		//    打标记 是否满足预警条件  整理格式 整理成预警格式
		//    3  三次及以上用不同账号登录并领取优惠劵
		//    4  领劵过程中没有浏览商品
		val checkAlertInfoDStream: DStream[(Boolean, AlertInfo)] = windowDStream.map { case (mid, eventinfoiter) =>
			//前面根据mid进行分组后，这里统计的全部是一个mid之内的数据
			//同一台设备领取优惠券的用户数
			val couponUidSet: util.HashSet[String] = new util.HashSet[String]()
			//商品id
			val itemsIdsSet: util.HashSet[String] = new util.HashSet[String]()
			//有那些事件
			val eventInfoList = new util.ArrayList[String]()
			//是否发生过点击事件
			var clickItemFlag: Boolean = false


			breakable(
				//循环每一条记录
				for (eventinfo: EventInfo <- eventinfoiter) {

					//把记录的事件放进事件的集合
					eventInfoList.add(eventinfo.evid)
					//如果是领优惠券的事件把uid放进uid的集合，商品id 放进商品id的集合
					if (eventinfo.evid == "coupon") {
						couponUidSet.add(eventinfo.uid)
						itemsIdsSet.add(eventinfo.itemid)
						//如果是点击事件  标记点击 切此设备不再处理 放过
					} else if (eventinfo.evid == "clickItem") {
						clickItemFlag = true
						break
					}
				}

			)

			val alertInfo = AlertInfo(mid, couponUidSet, itemsIdsSet, eventInfoList, System.currentTimeMillis())
			(couponUidSet.size() >= 3 && !clickItemFlag, alertInfo)

		}

	/*	checkAlertInfoDStream.foreachRDD{rdd=>
		      println(rdd.collect().mkString("\n"))

		    }*/
		//过滤不是预警的的数据
		val resultDStream: DStream[AlertInfo] = checkAlertInfoDStream.filter(_._1).map(_._2)

		//5 去重  依靠存储的容器进行去重  主键  mysql如果主键已存在会报异常
		//        利用ES的主键进行去重，主键已存在会覆盖之前的数据，这里对时间的要求不敏感。只要数据在就行
		//        一分钟内只保留一条数据   主键由  mid + 分钟  组成
		//把数据发送给ES

		resultDStream.foreachRDD { rdd =>
			rdd.foreachPartition { alertinfoiter => {
				//转换结构为键值对
				val alertInfoList: List[(String, AlertInfo)] = alertinfoiter.toList.map(alertinfo => {
					(alertinfo.mid + "-" + alertinfo.ts / 1000 / 60, alertinfo)
				})
				//打印出来
				println(alertInfoList.mkString("\n"))

				//EsUtil.insertBulk(GmallConstants.ES_INDEX_ALERT, alertInfoList)
			}
			}
		}


		ssc.start()
		ssc.awaitTermination()

	}
}
