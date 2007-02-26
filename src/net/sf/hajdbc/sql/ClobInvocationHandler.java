/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.sql;

import java.lang.reflect.Method;
import java.sql.Clob;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;

/**
 * @author Paul Ferraro
 *
 */
public class ClobInvocationHandler<D, P> extends AbstractInvocationHandler<D, P, Clob>
{
	private static final Set<String> DATABASE_READ_METHOD_SET = new HashSet<String>(Arrays.asList("getAsciiStream", "getCharacterStream", "getSubString", "length", "position"));
	private static final Set<String> DRIVER_WRITE_METHOD_SET = new HashSet<String>(Arrays.asList("free"));
	
	/**
	 * @param object
	 * @param proxy
	 * @param invoker
	 * @param objectMap
	 * @throws Exception
	 */
	protected ClobInvocationHandler(P object, SQLProxy<D, P> proxy, Invoker<D, P, Clob> invoker, Map<Database<D>, Clob> objectMap) throws Exception
	{
		super(object, proxy, invoker, objectMap);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<D, Clob, ?> getInvocationStrategy(Clob clob, Method method, Object[] parameters) throws Exception
	{
		String methodName = method.getName();
		
		if (DATABASE_READ_METHOD_SET.contains(methodName))
		{
			return new DatabaseReadInvocationStrategy<D, Clob, Object>();
		}
		
		if (DRIVER_WRITE_METHOD_SET.contains(methodName))
		{
			return new DriverWriteInvocationStrategy<D, Clob, Object>();
		}
		
		return super.getInvocationStrategy(clob, method, parameters);
	}
}
