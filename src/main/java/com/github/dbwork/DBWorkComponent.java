package com.github.dbwork;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.apache.camel.util.CamelContextHelper;
import org.apache.camel.util.IntrospectionSupport;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Map;


public class DBWorkComponent extends DefaultComponent {
    private DataSource dataSource;

    public DBWorkComponent() {
    }

    public DBWorkComponent(CamelContext context) {
        super(context);
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        String dataSourceRef = getAndRemoveParameter(parameters, "dataSourceRef", String.class);
        if (dataSourceRef != null) {
            dataSource = CamelContextHelper.mandatoryLookup(getCamelContext(), dataSourceRef, DataSource.class);
        }
        
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        IntrospectionSupport.setProperties(jdbcTemplate, parameters, "template.");

        String queueName = remaining;

        return new DBWorkEndpoint(uri, this, jdbcTemplate, queueName, remaining);
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DataSource getDataSource() {
        return dataSource;
    }
}
