package com.github.dbwork;

import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultPollingEndpoint;
import org.springframework.jdbc.core.JdbcTemplate;


public class DBWorkEndpoint extends DefaultPollingEndpoint {
    private JdbcTemplate jdbcTemplate;
    private String queueName;
    private String remaining;

    public DBWorkEndpoint() {
    }

    public DBWorkEndpoint(String uri, Component component, JdbcTemplate jdbcTemplate, String queueName, String remaining) {
        super(uri, component);
        this.jdbcTemplate = jdbcTemplate;
        this.queueName = queueName;
        this.remaining = remaining;
    }

    public Consumer createConsumer(Processor processor) throws Exception {
        return new DBWorkConsumer(this, processor, jdbcTemplate);
    }

    public Producer createProducer() throws Exception {
        return new DBWorkProducer(this, queueName, jdbcTemplate);
    }

    public boolean isSingleton() {
        return true;
    }

    @Override
    protected String createEndpointUri() {
        return "sql:" + queueName;
    }

    public String getRemaining() {
        return remaining;
    }
}
