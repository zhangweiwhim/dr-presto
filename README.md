<font size=2>

# dr_presto
presto审计方案

## 部署方案

```text
query_info 的采集是在 master 上执行
spilt_info 的采集是在每台 worker 上执行
==> master 和 worker 都需要部署
 ```

### step1 创建 etc/event-listener.properties

```properties
event-listener.name=query-event-listener-with-zhangWei
jdbc.uri=jdbc:mysql://XXX:XXX/dr_presto
jdbc.user=XXXX
jdbc.pwd=XXXX
#true则写入mysql，false写入日志文件中
is.database=false 
```


### step2 文件准备

- presto 的 plugin目录下创建 query-event-listener-with-zhangWei 目录
- 将jar包和mysql connector 等用到的jar包copy到query-event-listener-with-zhangWei 目录
- 重启presto 服务

## 涉及到入库指标
全收取


