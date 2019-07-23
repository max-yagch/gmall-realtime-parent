package yang.gmallrealtimepubliseher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

	public double getOrderAmountTotal(String date);

	public List<Map> getOrderHourTotal(String date);

}
