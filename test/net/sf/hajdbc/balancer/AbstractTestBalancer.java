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
package net.sf.hajdbc.balancer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;

import net.sf.hajdbc.ActiveDatabaseMBean;
import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.InactiveDatabaseMBean;
import net.sf.hajdbc.sql.AbstractDatabase;

import org.testng.annotations.Configuration;
import org.testng.annotations.Test;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public abstract class AbstractTestBalancer
{
	private Balancer balancer = createBalancer();
	
	protected abstract Balancer createBalancer();

	@Configuration(afterTestMethod = true)
	protected void tearDown()
	{
		for (Database database: new ArrayList<Database>(this.balancer.all()))
		{
			this.balancer.remove(database);
		}
	}
	
	/**
	 * Test method for {@link Balancer#add(Database)}
	 */
	@Test
	public void testAdd()
	{
		Database database = new MockDatabase("1", 1);
		
		boolean added = this.balancer.add(database);
		
		assert added;

		added = this.balancer.add(database);

		assert !added;
	}

	/**
	 * Test method for {@link Balancer#beforeOperation(Database)}
	 */
	@Test
	public void testBeforeOperation()
	{
		Database database = new MockDatabase("db1", 1);

		this.balancer.add(database);
		
		this.balancer.beforeOperation(database);
	}

	/**
	 * Test method for {@link Balancer#afterOperation(Database)}
	 */
	@Test
	public void testAfterOperation()
	{
		Database database = new MockDatabase("db1", 1);

		this.balancer.add(database);
		
		this.balancer.afterOperation(database);
	}
	
	/**
	 * Test method for {@link Balancer#remove(Database)}
	 */
	@Test
	public void testRemove()
	{
		Database database = new MockDatabase("1", 1);
		
		boolean removed = this.balancer.remove(database);

		assert !removed;
		
		this.balancer.add(database);

		removed = this.balancer.remove(database);

		assert removed;
		
		removed = this.balancer.remove(database);

		assert !removed;
	}

	/**
	 * Test method for {@link Balancer#contains(Database)}
	 */
	@Test
	public void testGetDatabases()
	{
		Collection<Database> databases = this.balancer.all();
		
		assert databases.isEmpty() : databases.size();
		
		Database database1 = new MockDatabase("db1", 1);
		this.balancer.add(database1);
		
		databases = this.balancer.all();
		
		assert databases.size() == 1 : databases.size();
		assert databases.contains(database1);
		
		Database database2 = new MockDatabase("db2", 1);
		this.balancer.add(database2);

		databases = this.balancer.all();

		assert databases.size() == 2 : databases.size();
		assert databases.contains(database1) && databases.contains(database2);

		this.balancer.remove(database1);

		databases = this.balancer.all();
		
		assert databases.size() == 1 : databases.size();
		assert databases.contains(database2);
		
		this.balancer.remove(database2);
		
		databases = this.balancer.all();
		
		assert databases.isEmpty() : databases.size();
	}

	/**
	 * Test method for {@link Balancer#contains(Database)}
	 */
	@Test
	public void testContains()
	{
		Database database1 = new MockDatabase("db1", 1);
		Database database2 = new MockDatabase("db2", 1);

		this.balancer.add(database1);
		
		assert this.balancer.contains(database1);
		assert !this.balancer.contains(database2);
	}

	/**
	 * Test method for {@link Balancer#first()}
	 */
	@Test
	public void testFirst()
	{
		try
		{
			this.balancer.first();
			
			assert false;
		}
		catch (NoSuchElementException e)
		{
			assert true;
		}
		
		Database database = new MockDatabase("0", 0);
		
		this.balancer.add(database);
		
		Database first = this.balancer.first();

		assert database.equals(first) : database;
	}

	/**
	 * Test method for {@link Balancer#next()}
	 */
	@Test
	public void testNext()
	{
		try
		{
			Database database = this.balancer.next();
			
			assert false : database.getId();
		}
		catch (NoSuchElementException e)
		{
			assert true;
		}
		
		testNext(this.balancer);
	}
	
	protected abstract void testNext(Balancer balancer);
	
	protected class MockDatabase extends AbstractDatabase<Void>
	{
		protected MockDatabase(String id, int weight)
		{
			this.id = id;
			this.weight = weight;
		}

		/**
		 * @see net.sf.hajdbc.Database#connect(T)
		 */
		public Connection connect(Void connectionFactory) throws SQLException
		{
			return null;
		}

		/**
		 * @see net.sf.hajdbc.Database#createConnectionFactory()
		 */
		public Void createConnectionFactory() throws SQLException
		{
			return null;
		}

		/**
		 * @see net.sf.hajdbc.Database#getConnectionFactoryClass()
		 */
		public Class<Void> getConnectionFactoryClass()
		{
			return null;
		}

		/**
		 * @see net.sf.hajdbc.Database#getActiveMBeanClass()
		 */
		public Class<? extends ActiveDatabaseMBean> getActiveMBeanClass()
		{
			return null;
		}

		/**
		 * @see net.sf.hajdbc.Database#getInactiveMBeanClass()
		 */
		public Class<? extends InactiveDatabaseMBean> getInactiveMBeanClass()
		{
			return null;
		}
	}
}
