package yang.canal.handler;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import yang.canal.KafkaUtil.KafkaSender;
import yang.common.GmallConstants;


import java.util.List;

public class CanalHandler {

		String tableName;
		CanalEntry.EventType eventType;
		List<CanalEntry.RowData> rowDataList;


	public CanalHandler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
		this.tableName = tableName;
		this.eventType = eventType;
		this.rowDataList = rowDataList;
	}


	public void handle(){
		if (tableName.equals("order_info")&&eventType==CanalEntry.EventType.INSERT){
			//遍历行集
			for (CanalEntry.RowData rowData : rowDataList) {
				List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
				JSONObject jsonObject = new JSONObject();
				for (CanalEntry.Column column : afterColumnsList) {
					System.out.println(column.getName() + ":::::" + column.getValue());

					jsonObject.put(column.getName(), column.getValue());
				}
				//发送kafka
				KafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());
			}
		}
	}
}
