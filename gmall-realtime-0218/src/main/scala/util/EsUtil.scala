package util

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object EsUtil {


	private val ES_HOST = "http://hadoop102"
	private val ES_PORT = 9200
	private var factory: JestClientFactory = null


	/*
	建立连接
	 */

	private def build(): Unit = {
		//创建一个工厂
		factory = new JestClientFactory
		factory.setHttpClientConfig(new HttpClientConfig.
		Builder(ES_HOST + ":" + ES_PORT).
				multiThreaded(true).
				maxTotalConnection(20).
				connTimeout(10000).
				readTimeout(10000).
				build())


	}

	/*
	获取客户端
	 */
	def getClient: JestClient = {
		if (factory == null) {
			build()
		}
		factory.getObject
	}



	/*
	关闭客户端，使用client自带的close会经常出问题
	 */

	def close(client: JestClient): Unit = {
		if (!Objects.isNull(client)) {
			try {
				client.shutdownClient()
			} catch {
				case e: Exception => e.printStackTrace()
			}
		}
	}




	//向ES 插入数据batch 处理

	def insertBulk(indexName:String,list:List[(String,Any)])={
		if (list.size>0){
			val jest: JestClient = getClient
			val bulkBuilder = new Bulk.Builder

			bulkBuilder.defaultIndex(indexName).defaultType("_doc")

			for ((id,doc) <- list) {

				//先构造成index  插入操作

				val index: Index = new Index.Builder(doc).id(id).build()

				bulkBuilder.addAction(index)
			}


			val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems

			println("保存 " + items.size() + " 条数据")

			close(jest)




		}
	}

	/*//用于写入数据测试   要先在ES创建表gmall_alert_info
	def main(args: Array[String]): Unit = {
		val jest: JestClient = EsUtil.getClient
		val index: Index = new Index.Builder(alertInfo3("wang5","shanghai")).index("gmall_alert_info").`type`("_doc").id("4").build()
		jest.execute(index)
		close(jest)
	}
	case class alertInfo3(name:String ,address:String)*/
}
