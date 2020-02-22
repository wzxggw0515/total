package yx.sz.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * @作者  王泽鑫
 *
 * @时间 2019年8月4日
 */
@TableName("u_user")
public class User {
	@TableId(type=IdType.AUTO,value="uid")
	private Integer uid;
	
	@TableField("uname")
	private String uname;
	
	@TableField("pwd")
	private String pwd;
	
	@TableField("paths")
	private String paths;

	public User() {
		super();
		// TODO Auto-generated constructor stub
	}

	public User(Integer uid, String uname, String pwd, String paths) {
		super();
		this.uid = uid;
		this.uname = uname;
		this.pwd = pwd;
		this.paths = paths;
	}

	public Integer getUid() {
		return uid;
	}

	public void setUid(Integer uid) {
		this.uid = uid;
	}

	public String getUname() {
		return uname;
	}

	public void setUname(String uname) {
		this.uname = uname;
	}

	public String getPwd() {
		return pwd;
	}

	public void setPwd(String pwd) {
		this.pwd = pwd;
	}

	public String getPaths() {
		return paths;
	}

	public void setPaths(String paths) {
		this.paths = paths;
	}

	@Override
	public String toString() {
		return "User [uid=" + uid + ", uname=" + uname + ", pwd=" + pwd + ", paths=" + paths + "]";
	}
	
	
	
}
