package yx.sz.service;

import org.springframework.stereotype.Service;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import yx.sz.bean.User;
import yx.sz.dao.UserDao;

/**
 * @作者  王泽鑫
 *
 * @时间 2019年8月4日
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserDao, User> implements UserService{

}
