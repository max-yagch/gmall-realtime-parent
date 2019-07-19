package yang.gmallrealtimelogger.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

	@Controller
	public class LogJsonController {
		@ResponseBody
		@RequestMapping("testDemo")
		public String testDemo(){
			return "hello demo";
		}
	}

