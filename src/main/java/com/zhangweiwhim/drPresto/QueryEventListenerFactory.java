package com.zhangweiwhim.drPresto;

import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;

import java.util.Map;

/**
 * Description: dr-presto
 * Created by @author zhangWei on 2023/2/20 21:42
 */
public class QueryEventListenerFactory implements EventListenerFactory {
    @Override
    public String getName() {
        return "query-event-listener-with-zhangWei";
    }

    @Override
    public EventListener create(Map<String, String> config) {
        if (!config.containsKey("is.database")) {
            throw new RuntimeException("/etc/event-listener.properties file missing is.database");
        }
        if (!config.containsKey("jdbc.uri")) {
            throw new RuntimeException("/etc/event-listener.properties file missing jdbc.uri");
        }
        if (!config.containsKey("jdbc.user")) {
            throw new RuntimeException("/etc/event-listener.properties file missing jdbc.user");
        }
        if (!config.containsKey("jdbc.pwd")) {
            throw new RuntimeException("/etc/event-listener.properties file missing jdbc.pwd");
        }

        return new QueryEventListener(config);
    }
}
