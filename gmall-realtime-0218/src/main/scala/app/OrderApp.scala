package app

import bean.OrderInfo
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.KafkaUtil
import yang.common.GmallConstants
import org.apache.phoenix.spark._

object OrderApp {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")
		val ssc = new StreamingContext(conf, Seconds(5))
		//kafka读取数据。key value类型
		val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

		val orderInfoDStream: DStream[OrderInfo] = inputDStream.map { record =>
			//取数据封装成对象
			val jsonString: String = record.value()

			val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
			//手机号脱敏
			orderInfo.consignee_tel = orderInfo.consignee_tel.splitAt(3)._1 + "********"
			//补充时间的两个字段
			val dateTimeArr: Array[String] = orderInfo.create_time.split(" ")

			orderInfo.create_date = dateTimeArr(0)

			val timeArr: Array[String] = dateTimeArr(1).split(":")

			orderInfo.create_hour = timeArr(0)

			orderInfo

		}
		//写进hbase
		orderInfoDStream.foreachRDD{rdd=>
			rdd.saveToPhoenix("GMALL_REALTIME_ORDER_INFO",
				Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
				new Configuration,
				Some("hadoop102,hadoop103,hadoop104:2181"))
		}

		ssc.start()
		ssc.awaitTermination()

	}
}
