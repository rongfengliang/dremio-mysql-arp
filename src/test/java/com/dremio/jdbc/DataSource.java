package com.dremio.jdbc;

import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.conf.MyMySQLConf;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class DataSource {
    private static final Logger logger = LoggerFactory.getLogger(MyMySQLConf.class);
    private static final String MYSQLDRIVER = "com.mysql.jdbc.Driver";

    public String hostname;

    public String port = "3306";
    public String username;

    public String password;

    public AuthenticationType authenticationType;

    public int fetchSize = 200;

    public int netWriteTimeout = 60;

    @JsonIgnore
    public boolean useLegacyDialect = false;

    public boolean enableExternalQuery = false;

    public List<Property> propertyList;

    public int maxIdleConns = 8;

    public int idleTimeSec = 60;

    public int queryTimeoutSec = 0;


    public CloseableDataSource newDataSource() {
        Properties properties = new Properties();
        properties.put("useJDBCCompliantTimezoneShift", "true");
        properties.put("sessionVariables", String.format("net_write_timeout=%d", this.netWriteTimeout));
        return DataSources.newGenericConnectionPoolDataSource(MYSQLDRIVER, this.toJdbcConnectionString(), this.username, this.password, properties, DataSources.CommitMode.FORCE_MANUAL_COMMIT_MODE, this.maxIdleConns, (long) this.idleTimeSec);
    }

    @VisibleForTesting
    public String toJdbcConnectionString() {
        String hostname = (String) Preconditions.checkNotNull(this.hostname, "missing hostname");
        String portAsString = (String) Preconditions.checkNotNull(this.port, "missing port");
        int port = Integer.parseInt(portAsString);
        String url = String.format("jdbc:mysql://%s:%d", hostname, port);
        logger.info("url:{}", url);
        return null != this.propertyList && !this.propertyList.isEmpty() ? url + (String) this.propertyList.stream().map((p) -> {
            return p.name + "=" + p.value;
        }).collect(Collectors.joining("&", "?", "")) : url;

    }
}
