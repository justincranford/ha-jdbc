/*
 * Copyright (c) 2004-2007, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.dialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class TestH2Dialect extends TestStandardDialect
{
	@Override
	protected Dialect createDialect()
	{
		return new H2Dialect();
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Override
	@Test(dataProvider = "foreign-key")
	public String getCreateForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getCreateForeignKeyConstraintSQL(constraint);
		
		this.verify();
		
		assert sql.equals("ALTER TABLE table ADD CONSTRAINT name FOREIGN KEY (column1, column2) REFERENCES foreign_table (foreign_column1, foreign_column2) ON DELETE CASCADE ON UPDATE RESTRICT") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getSequences(java.sql.DatabaseMetaData)
	 */
	@Override
	@Test(dataProvider = "meta-data")
	public Collection<QualifiedName> getSequences(DatabaseMetaData metaData) throws SQLException
	{
		EasyMock.expect(metaData.getConnection()).andReturn(this.connection);
		EasyMock.expect(this.connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT SEQUENCE_SCHEMA, SEQUENCE_NAME FROM INFORMATION_SCHEMA.SYSTEM_SEQUENCES")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("schema1");
		EasyMock.expect(this.resultSet.getString(2)).andReturn("sequence1");
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("schema2");
		EasyMock.expect(this.resultSet.getString(2)).andReturn("sequence2");
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		
		this.statement.close();
		
		this.replay();
		
		Collection<QualifiedName> sequences = this.dialect.getSequences(metaData);
		
		this.verify();
		
		assert sequences.size() == 2 : sequences;
		
		Iterator<QualifiedName> iterator = sequences.iterator();
		QualifiedName sequence = iterator.next();
		String schema = sequence.getSchema();
		String name = sequence.getName();
		
		assert schema.equals("schema1") : schema;
		assert name.equals("sequence1") : name;
		
		sequence = iterator.next();
		schema = sequence.getSchema();
		name = sequence.getName();
		
		assert schema.equals("schema2") : schema;
		assert name.equals("sequence2") : name;
		
		return sequences;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getSimpleSQL()
	 */
	@Override
	@Test
	public String getSimpleSQL() throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getSimpleSQL();
		
		this.verify();
		
		assert sql.equals("CALL CURRENT_TIMESTAMP") : sql;
		
		return sql;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getCurrentSequenceValueSQL(java.lang.String)
	 */
	@Test(dataProvider = "sequence")
	@Override
	public String getNextSequenceValueSQL(SequenceProperties sequence) throws SQLException
	{
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		this.replay();
		
		String sql = this.dialect.getNextSequenceValueSQL(sequence);
		
		this.verify();
		
		assert sql.equals("CALL NEXT VALUE FOR sequence") : sql;
		
		return sql;
	}
	
	@Override
	@DataProvider(name = "current-date")
	Object[][] currentDateProvider()
	{
		java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
		
		return new Object[][] {
			new Object[] { "SELECT CURRENT_DATE FROM success", date },
			new Object[] { "SELECT CURRENT_DATE() FROM success", date },
			new Object[] { "SELECT CURRENT_DATE ( ) FROM success", date },
			new Object[] { "SELECT CURDATE() FROM success", date },
			new Object[] { "SELECT CURDATE ( ) FROM success", date },
			new Object[] { "SELECT CURRENT_DATES FROM failure", date },
			new Object[] { "SELECT CCURRENT_DATE FROM failure", date },
			new Object[] { "SELECT CCURDATE() FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}

	@Override
	@DataProvider(name = "current-time")
	Object[][] currentTimeProvider()
	{
		java.sql.Time date = new java.sql.Time(System.currentTimeMillis());
		
		return new Object[][] {
			new Object[] { "SELECT CURRENT_TIME FROM success", date },
			new Object[] { "SELECT CURRENT_TIME() FROM success", date },
			new Object[] { "SELECT CURRENT_TIME ( ) FROM success", date },
			new Object[] { "SELECT CURTIME() FROM success", date },
			new Object[] { "SELECT CURTIME ( ) FROM success", date },
			new Object[] { "SELECT CCURRENT_TIME FROM failure", date },
			new Object[] { "SELECT CURRENT_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT CCURTIME() FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}

	@Override
	@DataProvider(name = "current-timestamp")
	Object[][] currentTimestampProvider()
	{
		java.sql.Timestamp date = new java.sql.Timestamp(System.currentTimeMillis());
		
		return new Object[][] {
			new Object[] { "SELECT CURRENT_TIMESTAMP FROM success", date },
			new Object[] { "SELECT CURRENT_TIMESTAMP() FROM success", date },
			new Object[] { "SELECT CURRENT_TIMESTAMP ( ) FROM success", date },
			new Object[] { "SELECT NOW() FROM success", date },
			new Object[] { "SELECT NOW ( ) FROM success", date },
			new Object[] { "SELECT CURRENT_TIMESTAMPS FROM failure", date },
			new Object[] { "SELECT CCURRENT_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT NNOW() FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}
}
