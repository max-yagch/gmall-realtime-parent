package yang.gmallrealtimepubliseher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
@MapperScan(basePackages = "yang.gmallrealtimepubliseher.mapper")
@SpringBootApplication
public class GmallRealtimePubliseherApplication {

	public static void main(String[] args) {
		SpringApplication.run(GmallRealtimePubliseherApplication.class, args);
	}

}
