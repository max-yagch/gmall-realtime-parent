package yang.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import sun.jvm.hotspot.code.Location;
import yang.canal.handler.CanalHandler;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
	public static void main(String[] args) {
		//创建连接器
		CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");
		while (true) {
			//连接
			canalConnector.connect();
			//抓取的表
			canalConnector.subscribe("gmall_realtime.order_info");
			//抓取
			Message message = canalConnector.get(100);

			//判断有没有读取数据
			if (message.getEntries().size() == 0) {
				System.out.println("没有数据休息一会");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				for (CanalEntry.Entry entry : message.getEntries()) {
					//每个entry对应一个sql
					//过滤entry 因为不是每个sql都是对数据进行修改的写操作
					if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
						CanalEntry.RowChange rowChange = null;
						try {
							//把storeValue反序列化得到rowChange
							rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
						} catch (InvalidProtocolBufferException e) {
							e.printStackTrace();
						}


						List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

						CanalEntry.EventType eventType = rowChange.getEventType();

						String tableName = entry.getHeader().getTableName();


						CanalHandler canalHandler = new CanalHandler(tableName, eventType, rowDatasList);
						canalHandler.handle();
					}
				}
			}


		}


	}
}
