package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcPluginConfig.Builder;
import com.dremio.exec.store.jdbc.dialect.MyMySQLDialect;
import com.dremio.options.OptionManager;
import com.dremio.services.credentials.CredentialsService;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import io.protostuff.Tag;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import javax.sql.ConnectionPoolDataSource;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SourceType(
        value = "MYMYSQL",
        label = "MYMYSQL",
        uiConfig = "my-mysql-layout.json",
        externalQuerySupported = true,
        previewEngineRequired = true
)
public class MyMySQLConf extends AbstractArpConf<MyMySQLConf> {
    private static final Logger logger = LoggerFactory.getLogger(MySQLConf.class);
    private static final String ARP_FILENAME = "arp/implementation/mysql-arp.yaml";
    private static final MyMySQLDialect MYSQL_ARP_DIALECT = (MyMySQLDialect)AbstractArpConf.loadArpFile(ARP_FILENAME, MyMySQLDialect::new);
    private static final String POOLED_DATASOURCE = "org.mariadb.jdbc.MariaDbDataSource";
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
    @Tag(3)
    @DisplayMetadata(
            label = "Database"
    )
    public String database;
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
    public JdbcPluginConfig buildPluginConfig(Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
        return configBuilder.withDialect(this.getDialect()).withDatasourceFactory(this::newDataSourcev2).withShowOnlyConnDatabase(false).withFetchSize(this.fetchSize).withQueryTimeout(this.queryTimeoutSec).build();
    }

    private CloseableDataSource newDataSource() throws SQLException {
        ConnectionPoolDataSource source;
        try {
            source = (ConnectionPoolDataSource)Class.forName("org.mariadb.jdbc.MariaDbDataSource").newInstance();
        } catch (ReflectiveOperationException var3) {
            throw new RuntimeException("Cannot instantiate MySQL datasource", var3);
        }

        return this.newDataSource(source);
    }
    @VisibleForTesting
    CloseableDataSource newDataSourcev2()  {
        Properties properties = new Properties();
        properties.put("useJDBCCompliantTimezoneShift", "true");
        properties.put("sessionVariables", String.format("net_write_timeout=%d", this.netWriteTimeout));
        return DataSources.newGenericConnectionPoolDataSource(MYSQLDRIVER, this.toJdbcConnectionStringv2(), this.username, this.password, properties, DataSources.CommitMode.FORCE_MANUAL_COMMIT_MODE, this.maxIdleConns, (long)this.idleTimeSec);
    }

    @VisibleForTesting
    String toJdbcConnectionStringv2() {
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


    @VisibleForTesting
    CloseableDataSource newDataSource(ConnectionPoolDataSource source) throws SQLException {
        String url = this.toJdbcConnectionString();

        try {
            logger.debug("Creating a MySQL/MariaDB data source with URL: " + url);
            MethodUtils.invokeExactMethod(source, "setUrl", new Object[]{url});
            if (this.username != null) {
                MethodUtils.invokeExactMethod(source, "setUser", new Object[]{this.username});
            }

            if (this.password != null) {
                MethodUtils.invokeExactMethod(source, "setPassword", new Object[]{this.password});
            }

            return DataSources.newSharedDataSource(source, this.maxIdleConns, (long)this.idleTimeSec);
        } catch (InvocationTargetException var5) {
            Throwable cause = var5.getCause();
            if (cause != null) {
                Throwables.throwIfInstanceOf(cause, SQLException.class);
            }

            throw new RuntimeException("Cannot instantiate MySQL datasource", var5);
        } catch (ReflectiveOperationException var6) {
            throw new RuntimeException("Cannot instantiate MySQL datasource", var6);
        }
    }

    @VisibleForTesting
    String toJdbcConnectionString() {
        String hostname = (String)Preconditions.checkNotNull(this.hostname, "missing hostname");
        String portAsString = (String)Preconditions.checkNotNull(this.port, "missing port");
        int port = Integer.parseInt(portAsString);
        String url;
        if (!Strings.isNullOrEmpty(this.database)) {
            url = String.format("jdbc:mariadb://%s:%d/%s", hostname, port, this.database);
        } else {
            url = String.format("jdbc:mariadb://%s:%d", hostname, port);
        }

        List<Property> allProperties = new ArrayList();
        allProperties.add(MyMySQLConf.FixedPropertyNames.TIMEZONE.createProperty("true"));
        allProperties.add(MyMySQLConf.FixedPropertyNames.SESSION_VARIABLES.createProperty(String.format("net_write_timeout=%d", this.netWriteTimeout)));
        allProperties.add(MyMySQLConf.FixedPropertyNames.LOCAL_INFILE.createProperty("false"));
        if (null != this.propertyList) {
            this.propertyList.forEach((p) -> {
                String sanitizedName = sanitizePropertyName(p.name);
                String santizedValue = sanitizePropertyValue(p.value);
                if (!Strings.isNullOrEmpty(sanitizedName)) {
                    if (MyMySQLConf.FixedPropertyNames.exists(sanitizedName)) {
                        logger.debug("User provided property ({}={}) for source ({}) was removed", new Object[]{p.name, p.value, this.hostname});
                    } else {
                        allProperties.add(new Property(sanitizedName, santizedValue));
                    }
                }

            });
        }

        return url + (String)allProperties.stream().map((p) -> {
            return p.name + "=" + p.value;
        }).collect(Collectors.joining("&", "?", ""));
    }

    private static String sanitizePropertyName(String propertyName) {
        if (propertyName == null) {
            return null;
        } else {
            String sanitizedPropertyName = propertyName.replaceAll("=", "%3D").replaceAll("\\?", "%3F").replaceAll("&", "%26");
            if (!propertyName.equals(sanitizedPropertyName)) {
                logger.info("Property name was altered from ({} to {}).", propertyName, sanitizedPropertyName);
            }

            return sanitizedPropertyName;
        }
    }

    private static String sanitizePropertyValue(String value) {
        if (value == null) {
            return "";
        } else {
            String sanitizedValue = value.replaceAll("\\?", "%3F").replaceAll("&", "%26");
            if (!value.equals(sanitizedValue)) {
                logger.info("Property value was altered from ({} to {}).", value, sanitizedValue);
            }

            return sanitizedValue;
        }
    }

    public MyMySQLDialect getDialect() {
        return MYSQL_ARP_DIALECT;
    }

    @VisibleForTesting
    public static MyMySQLDialect getDialectSingleton() {
        return MYSQL_ARP_DIALECT;
    }

    private static enum FixedPropertyNames {
        TIMEZONE("useJDBCCompliantTimezoneShift"),
        SESSION_VARIABLES("sessionVariables"),
        LOCAL_INFILE("allowLocalInfile");

        private static final Set<String> LOWER_CASE_NAMES = (Set) Arrays.stream(values()).map((value) -> {
            return value.propertyName.toLowerCase();
        }).collect(ImmutableSet.toImmutableSet());
        private final String propertyName;

        private FixedPropertyNames(String propertyName) {
            this.propertyName = propertyName;
        }

        public Property createProperty(String value) {
            return new Property(this.propertyName, value);
        }

        public static boolean exists(String name) {
            return name != null && LOWER_CASE_NAMES.contains(name.toLowerCase());
        }
    }
}
