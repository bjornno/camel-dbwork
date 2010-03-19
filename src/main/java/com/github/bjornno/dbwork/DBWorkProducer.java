package com.github.bjornno.dbwork;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.springframework.jdbc.core.JdbcTemplate;

public class DBWorkProducer extends DefaultProducer {
    private String queueName;
    private JdbcTemplate jdbcTemplate;

    public DBWorkProducer(DBWorkEndpoint endpoint, String queueName, JdbcTemplate jdbcTemplate) {
        super(endpoint);
        this.jdbcTemplate = jdbcTemplate;
        this.queueName = queueName;
    }

    public void process(final Exchange exchange) throws Exception {
        jdbcTemplate.execute("insert into dbwork(id, workid, queuename) values('"+exchange.getExchangeId()+"', '"+exchange.getExchangeId()+"','"+queueName+"')");   
    }

}
