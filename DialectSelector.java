package com.nec.biomatcher.core.framework.dataAccess;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.hibernate.dialect.MySQL57InnoDBDialect;
import org.hibernate.dialect.PostgreSQL9Dialect;
import org.hibernate.dialect.PostgresPlusDialect;
import org.springframework.batch.support.DatabaseType;

import com.nec.biomatcher.core.framework.common.CommonLogger;

public class DialectSelector {
	private static final Logger logger = Logger.getLogger(DialectSelector.class);

	private DataSource dataSource;

	public String getDialectClass() {
		DatabaseType databaseType = null;

		try {
			databaseType = DatabaseType.fromMetaData(dataSource);
			CommonLogger.CONFIG_LOG.info("In DialectSelector: resolved databaseType: " + databaseType);
		} catch (Throwable th) {
			logger.error("Error determining the databaseType: " + th.getMessage(), th);
			throw new RuntimeException("Unable to determine databaseType from datasource : " + th.getMessage(), th);
		}

		// TODO: Need to add support for other database
		try {
			switch (databaseType) {
			case ORACLE:
				return Oracle10gDialect.class.getName();
			case MYSQL:
				return MySQL57InnoDBDialect.class.getName();
			case SQLSERVER:
				return SQLServer2014Dialect.class.getName();
			case POSTGRES:
				return PostgresPlusDialect.class.getName();
	

			default:
				throw new Exception(
						"Unable to determine the dialect class for databaseType: " + databaseType.getProductName());
			}
		} catch (Throwable th) {
			logger.error(th.getMessage(), th);
			throw new RuntimeException(th.getMessage(), th);
		}
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
}
