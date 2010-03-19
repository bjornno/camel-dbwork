package com.github.bjornno.dbwork;

import org.apache.camel.ExchangePattern;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.impl.DefaultMessage;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class DBWorkConsumer extends ScheduledPollConsumer {
    private DBWorkEndpoint endpoint;
    private JdbcTemplate jdbcTemplate;
    private final ScheduledThreadPoolExecutor executor;
    private final int concurrentConsumers = 6;

    public DBWorkConsumer(DBWorkEndpoint endpoint, Processor processor, JdbcTemplate jdbcTemplate) {
        super(endpoint, processor);

        this.endpoint = endpoint;
        this.jdbcTemplate = jdbcTemplate;
        this.executor = new ScheduledThreadPoolExecutor(this.concurrentConsumers);
    }

    @Override
    protected void poll() throws Exception {
        List workids = jdbcTemplate.queryForList("select workid from dbwork where queuename = '"+endpoint.getRemaining()+"' and status = 0", String.class);


        for (Iterator iterator = workids.iterator(); iterator.hasNext();) {
            String workid = (String) iterator.next();
            Task worker = new Task(endpoint, this.getProcessor(), workid, jdbcTemplate);
            executor.execute(worker);
            //executor.scheduleWithFixedDelay(worker, 0, 1, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    protected void doStop() throws Exception {
        executor.shutdown();
    }
}

class Task implements Runnable {
    private DBWorkEndpoint endpoint;
    private final Processor processor;
    private final String workId;
    private JdbcTemplate jdbcTemplate;

    public Task(DBWorkEndpoint endpoint, Processor processor, String workId, JdbcTemplate jdbcTemplate) throws Exception {
        this.endpoint = endpoint;
        this.processor = processor;
        this.workId = workId;
        this.jdbcTemplate = jdbcTemplate;

    }

    public void run() {
        DefaultExchange exchange = (DefaultExchange) endpoint.createExchange(ExchangePattern.InOut);
        Message message = new DefaultMessage();
        String body = jdbcTemplate.queryForObject("select text from resources where id = '"+workId+"'", String.class);
        message.setBody(body);
        exchange.setIn(message);
        try {
            processor.process(exchange);
        } catch (Exception e) {
            jdbcTemplate.update("update dbwork set status = 1 where workid = '" + workId + "'");
        }
        jdbcTemplate.update("update dbwork set status = 2 where workid = '" + workId + "'");
    }

}