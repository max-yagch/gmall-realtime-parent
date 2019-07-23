package yang.gmallrealtimepubliseher.service;

import java.util.Map;

public interface PublisherService {
	/*
	查询日活总数
	 */
	public int getDauTotal(String date);

	/*
	查询日活分时明细
	 */
	public Map getDauHour(String date);

	/*
	查询新增订单总金额
	 */

	public double getOrderAmountTotal(String date);


	/*
	查询新增订单分时金额
	 */

	public Map getOrderHourTotal(String date);

}
