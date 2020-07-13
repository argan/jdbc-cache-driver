/*
 * Copyright 2016 Emmanuel Keller / QWAZR
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.jdbc.cache;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver {

	public final static String URL_FILE_PREFIX = "jdbc:cache:file:";
	public final static String URL_MEM_PREFIX = "jdbc:cache:mem:";
	public final static String CACHE_DRIVER_URL = "cache.driver.url";
	public final static String CACHE_DRIVER_CLASS = "cache.driver.class";
	public final static String CACHE_DRIVER_ACTIVE = "cache.driver.active";
	public final static String CACHE_PATTERN_PREFIX = "cache.config.pattern.";
	final static Logger LOGGER = Logger.getLogger(Driver.class.getPackage().getName());

	static {
		try {
			DriverManager.registerDriver(new Driver());
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private final ConcurrentHashMap<String, ResultSetCache> resultSetCacheMap = new ConcurrentHashMap<>();

	public static ResultSetCache getCache(final Connection connection) throws SQLException {
		if (!(connection instanceof CachedConnection)) {
			throw new SQLException("The connection is not a cached connection");
		}
		return ((CachedConnection) connection).getResultSetCache();
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {

		if (!acceptsURL(url)) {
			return null;
		}

		// Determine the optional backend connection
		final String cacheDriverUrl = info.getProperty(CACHE_DRIVER_URL);
		final String cacheDriverClass = info.getProperty(CACHE_DRIVER_CLASS);
		try {
			if (cacheDriverClass != null && !cacheDriverClass.isEmpty()) {
				Class.forName(cacheDriverClass);
			}
		} catch (ClassNotFoundException e) {
			throw new SQLException("Cannot initialize the driver: " + cacheDriverClass, e);
		}

		final String cacheDriverActive = info.getProperty(CACHE_DRIVER_ACTIVE);
		final boolean active = cacheDriverActive == null || Boolean.parseBoolean(cacheDriverActive);

		final Connection backendConnection = cacheDriverUrl == null || cacheDriverUrl.isEmpty() ?
				null :
				DriverManager.getConnection(cacheDriverUrl, info);

		if (!active) {
			return new CachedConnection(backendConnection, null);
		}

		CacheConfig cacheConfig = buildCacheConfig(info);

		final ResultSetCache resultSetCache;
		if (url.startsWith(URL_FILE_PREFIX)) {
			if (url.length() <= URL_FILE_PREFIX.length()) {
				throw new SQLException("The path is empty: " + url);
			}
			// Check the cache directory
			final String cacheName = url.substring(URL_FILE_PREFIX.length());
			final Path cacheDirectory = FileSystems.getDefault().getPath(cacheName);
			resultSetCache = resultSetCacheMap.computeIfAbsent(cacheName, (foo) -> new ResultSetOnDiskCacheImpl(cacheDirectory, cacheConfig));
		} else if (url.startsWith(URL_MEM_PREFIX)) {
			if (url.length() <= URL_MEM_PREFIX.length()) {
				throw new SQLException("The name is empty: " + url);
			}
			// Check the cache directory
			final String cacheName = url.substring(URL_MEM_PREFIX.length());
			resultSetCache = resultSetCacheMap.computeIfAbsent(cacheName, (foo) -> new ResultSetInMemoryCacheImpl(cacheConfig));
		} else {
			throw new IllegalArgumentException("Can not find cache implementation for " + url);
		}

		return new CachedConnection(backendConnection, resultSetCache);
	}

	private CacheConfig buildCacheConfig(Properties info) {
		List<String> patternStrs = new ArrayList<String>();
		for (Object k : info.keySet()) {
			String key = (String) k;
			if (key.startsWith(CACHE_PATTERN_PREFIX)) {
				patternStrs.add(info.getProperty(key));
			}
		}
		if (!patternStrs.isEmpty()) {
			return new CacheConfig(patternStrs);
		}
		return null;
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url != null && (url.startsWith(URL_FILE_PREFIX) || url.startsWith(URL_MEM_PREFIX));
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) throws SQLException {
		return new DriverPropertyInfo[]{new DriverPropertyInfo(CACHE_DRIVER_URL, null),
				new DriverPropertyInfo(CACHE_DRIVER_CLASS, null), new DriverPropertyInfo(CACHE_DRIVER_ACTIVE, null)};
	}

	@Override
	public int getMajorVersion() {
		return 1;
	}

	@Override
	public int getMinorVersion() {
		return 3;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return LOGGER;
	}

}
