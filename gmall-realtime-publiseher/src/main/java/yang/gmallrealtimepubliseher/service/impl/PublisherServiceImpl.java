package yang.gmallrealtimepubliseher.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import yang.gmallrealtimepubliseher.mapper.DauMapper;
import yang.gmallrealtimepubliseher.mapper.OrderMapper;
import yang.gmallrealtimepubliseher.service.PublisherService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {
	@Autowired
	DauMapper dauMapper;
	@Autowired
	OrderMapper orderMapper;

	@Override
	public int getDauTotal(String date) {
		int dauTotal = dauMapper.getDauTotal(date);
		return dauTotal;
	}

	@Override
	public Map getDauHour(String date) {
		List<Map> dauHourList = dauMapper.getDauHour(date);
		//把 List<Map> 转换成  Map
		Map duaHourMap = new HashMap();
		for (Map map : dauHourList) {
			String loghour = (String) map.get("LOGHOUR");
			Long ct = (Long) map.get("CT");
			duaHourMap.put(loghour, ct);

		}
		return duaHourMap;
	}

	@Override
	public double getOrderAmountTotal(String date) {
		return orderMapper.getOrderAmountTotal(date);
	}

	@Override
	public Map getOrderHourTotal(String date) {
		List<Map> orderHourTotal = orderMapper.getOrderHourTotal(date);
		Map orderHourMap = new HashMap();
		for (Map map : orderHourTotal) {
			orderHourMap.put(map.get("CREATE_HOUR"), map.get("TOTAL_AMOUNT"));

		}


		return orderHourMap;
	}
}
