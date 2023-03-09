package com.zhangweiwhim.drPresto;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import com.google.common.collect.ImmutableList;


/**
 * Description: dr-presto
 * Created by @author zhangWei on 2023/2/20 21:54
 */
public class QueryEventPlugin implements Plugin {
    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories() {
        EventListenerFactory listenerFactory = new QueryEventListenerFactory();
        return ImmutableList.of(listenerFactory);
    }
}
