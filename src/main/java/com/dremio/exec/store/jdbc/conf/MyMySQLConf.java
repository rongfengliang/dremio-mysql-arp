package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.*;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcPluginConfig.Builder;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.dialect.MyMySQLDialect;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.protostuff.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@SourceType(
        value = "MYMYSQL",
        label = "MYMYSQL",
        uiConfig = "my-mysql-layout.json",
        externalQuerySupported = true
)
public class MyMySQLConf extends AbstractArpConf<MyMySQLConf> {
    private static final Logger logger = LoggerFactory.getLogger(MyMySQLConf.class);
    private static final String ARP_FILENAME = "arp/implementation/my-mysql-arp.yaml";
    private static final MyMySQLDialect MYSQL_ARP_DIALECT = AbstractArpConf.loadArpFile(ARP_FILENAME, MyMySQLDialect::new);
    private static final String MYSQLDRIVER = "com.mysql.jdbc.Driver";
    @NotBlank
    @Tag(1)
    @DisplayMetadata(
            label = "Host"
    )
    public String hostname;
    @NotBlank
    @Tag(2)
    @Min(1L)
    @Max(65535L)
    @DisplayMetadata(
            label = "Port"
    )
    public String port = "3306";
    @Tag(4)
    public String username;
    @Tag(5)
    @Secret
    public String password;
    @Tag(6)
    public AuthenticationType authenticationType;
    @Tag(7)
    @DisplayMetadata(
            label = "Record fetch size"
    )
    @NotMetadataImpacting
    public int fetchSize = 200;
    @Tag(8)
    @DisplayMetadata(
            label = "Net write timeout (in seconds)"
    )
    @NotMetadataImpacting
    public int netWriteTimeout = 60;
    @Tag(9)
    @DisplayMetadata(
            label = "Enable legacy dialect"
    )
    @JsonIgnore
    public boolean useLegacyDialect = false;
    @Tag(10)
    @NotMetadataImpacting
    @JsonIgnore
    public boolean enableExternalQuery = false;
    @Tag(11)
    public List<Property> propertyList;
    @Tag(12)
    @DisplayMetadata(
            label = "Maximum idle connections"
    )
    @NotMetadataImpacting
    public int maxIdleConns = 8;
    @Tag(13)
    @DisplayMetadata(
            label = "Connection idle time (s)"
    )
    @NotMetadataImpacting
    public int idleTimeSec = 60;
    @Tag(14)
    @DisplayMetadata(
            label = "Query timeout (s)"
    )
    @NotMetadataImpacting
    public int queryTimeoutSec = 0;

    @VisibleForTesting
    CloseableDataSource newDataSource()  {
        Properties properties = new Properties();
        properties.put("useJDBCCompliantTimezoneShift", "true");
        properties.put("sessionVariables", String.format("net_write_timeout=%d", this.netWriteTimeout));
        return DataSources.newGenericConnectionPoolDataSource(MYSQLDRIVER, this.toJdbcConnectionString(), this.username, this.password, properties, DataSources.CommitMode.FORCE_MANUAL_COMMIT_MODE, this.maxIdleConns, (long)this.idleTimeSec);
    }

    @VisibleForTesting
    String toJdbcConnectionString() {
        String hostname = (String)Preconditions.checkNotNull(this.hostname, "missing hostname");
        String portAsString = (String)Preconditions.checkNotNull(this.port, "missing port");
        int port = Integer.parseInt(portAsString);
        String url = String.format("jdbc:mysql://%s:%d", hostname, port);
        logger.info("url:{}",url);
        System.out.println("url"+url);
        String connectUrl =  null != this.propertyList && !this.propertyList.isEmpty() ? url + (String)this.propertyList.stream().map((p) -> {
            return p.name + "=" + p.value;
        }).collect(Collectors.joining("&", "?", "")) : url;
        System.out.println("connectUrl"+url);
        return  connectUrl;

    }

    @Override
    public JdbcDremioSqlDialect getDialect() {
        return this.MYSQL_ARP_DIALECT;
    }

    @Override
    public JdbcPluginConfig buildPluginConfig(Builder builder, CredentialsService credentialsService, OptionManager optionManager) {
      return builder.withDialect(this.getDialect()).withDatasourceFactory(this::newDataSource).withShowOnlyConnDatabase(false).withFetchSize(this.fetchSize).withQueryTimeout(this.queryTimeoutSec).build();
    }
}
