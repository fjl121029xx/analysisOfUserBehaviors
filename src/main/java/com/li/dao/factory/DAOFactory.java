package com.li.dao.factory;


import com.li.dao.ISessionAggrStatDAO;
import com.li.dao.ITaskDAO;
import com.li.dao.impl.SessionAggrStatDAOImpl;
import com.li.dao.impl.TaskDAOImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {


	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	

	
}
