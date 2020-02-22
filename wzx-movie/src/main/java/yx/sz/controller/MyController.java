package yx.sz.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.servlet.ModelAndView;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;

import yx.sz.bean.User;
import yx.sz.service.UserService;

/**
 * @作者  王泽鑫
 *
 * @时间 2019年8月3日
 */
@Controller
public class MyController {
	@Autowired
	private UserService ser;
	
	@GetMapping({"/","/index","/home"})
	public ModelAndView showview(){
		ModelAndView me = new ModelAndView("index");
		User user  = ser.getById(1);
		System.out.println(user);
		QueryWrapper<User> query = new QueryWrapper<User>();
		query.eq("uname", "liubei");
		User user1 = ser.getOne(query);
		System.out.println(user1+"----");
		return me;
	}
}
