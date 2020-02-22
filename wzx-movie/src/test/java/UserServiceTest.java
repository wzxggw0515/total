import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import yx.sz.bean.User;
import yx.sz.service.UserService;

/**
 * @作者  王泽鑫
 *
 * @时间 2019年8月4日
 */
@RunWith(SpringJUnit4ClassRunner.class)
@Configuration("classpath:spring-root-context.xml")
public class UserServiceTest {
	@Autowired
	private UserService ser;
	
	@Test
	public void Usertest(){
		User user  = ser.getById(1);
		System.out.println(user);
	}
	
}
