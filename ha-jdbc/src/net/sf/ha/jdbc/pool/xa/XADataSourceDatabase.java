package net.sf.ha.jdbc.pool.xa;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.XAConnection;
import javax.sql.XADataSource;

import net.sf.ha.jdbc.pool.ConnectionPoolDataSourceDatabase;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class XADataSourceDatabase extends ConnectionPoolDataSourceDatabase
{
	/**
	 * Constructs a new XADataSourceConnectionInfo.
	 * @param name
	 */
	public XADataSourceDatabase(String name)
	{
		super(name);
	}

	/**
	 * @see net.sf.ha.jdbc.ConnectionInfo#connect(java.lang.Object)
	 */
	public Connection connect(Object object) throws SQLException
	{
		XADataSource dataSource = (XADataSource) object;
		XAConnection connection = (this.user != null) ? dataSource.getXAConnection(this.user, this.password) : dataSource.getXAConnection();
		
		return this.getConnection(connection);
	}
}
