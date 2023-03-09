package com.zhangweiwhim.drPresto;

import com.alibaba.fastjson2.JSON;
import com.zaxxer.hikari.HikariDataSource;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Description: dr-presto
 * Created by @author zhangWei on 2023/2/20 21:45
 */
public class QueryEventListener implements EventListener {
    static final Logger infoLogger = LogManager.getLogger("infoLog");
    static final Logger failedLogger = LogManager.getLogger("failedLog");
    static final Logger planLogger = LogManager.getLogger("planLog");
    private Map<String, String> config;
    //    private Connection conn;
    private HikariDataSource hikariDataSource;

    public QueryEventListener(Map<String, String> config) {
        this.config = new HashMap<>();
        this.config.putAll(config);
        try {
            String isFile = config.get("is.database").toUpperCase();
            if (isFile.equals("TRUE")) {
                init();
            }
        } catch (Exception e) {
            e.getStackTrace();
        }

    }

    private void init() {
        if (hikariDataSource == null || !hikariDataSource.isRunning()) {
            HikariDataSource ds = new HikariDataSource();
            ds.setDriverClassName("com.mysql.jdbc.Driver");
            ds.setJdbcUrl(config.get("jdbc.uri"));
            ds.setUsername(config.get("jdbc.user"));
            ds.setPassword(config.get("jdbc.pwd"));
            ds.setMaximumPoolSize(100);
            ds.setMinimumIdle(10);
            ds.setPoolName("dr-presto");
            //                Class.forName("com.mysql.jdbc.Driver");
            //                conn = DriverManager.getConnection(config.get("jdbc.uri"), config.get("jdbc.user"),
            //                        config.get("jdbc.pwd"));
            hikariDataSource = ds;
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String queryId = queryCompletedEvent.getMetadata().getQueryId();
        String querySql = queryCompletedEvent.getMetadata().getQuery().replaceAll("\\n","").replaceAll("(\r\n|\r|\n|\n\r)","");
        String queryState = queryCompletedEvent.getMetadata().getQueryState();
        String queryUser = queryCompletedEvent.getContext().getUser();
        long analysisTime = queryCompletedEvent.getStatistics().getAnalysisTime().orElse(Duration.ZERO)
                .toMillis();
        long cpuTime = queryCompletedEvent.getStatistics().getCpuTime().toMillis();
        long queuedTime = queryCompletedEvent.getStatistics().getQueuedTime().toMillis();
        long wallTime = queryCompletedEvent.getStatistics().getWallTime().toMillis();
        int completedSplits = queryCompletedEvent.getStatistics().getCompletedSplits();
        double cumulativeMemory = queryCompletedEvent.getStatistics().getCumulativeMemory();
        long outputBytes = queryCompletedEvent.getStatistics().getOutputBytes();
        long outputRows = queryCompletedEvent.getStatistics().getOutputRows();
        long totalBytes = queryCompletedEvent.getStatistics().getTotalBytes();
        long totalRows = queryCompletedEvent.getStatistics().getTotalRows();
        long writtenBytes = queryCompletedEvent.getStatistics().getWrittenBytes();
        long writtenRows = queryCompletedEvent.getStatistics().getWrittenRows();
        String transactionId = queryCompletedEvent.getMetadata().getTransactionId().orElse("");
        String uri = queryCompletedEvent.getMetadata().getUri().toString();
        String plan = queryCompletedEvent.getMetadata().getPlan().orElse("").replaceAll("\\n","");
        String payload = queryCompletedEvent.getMetadata().getPayload().orElse("").replaceAll("\\n","");
        long waitingTime =
                queryCompletedEvent.getStatistics().getResourceWaitingTime().orElse(Duration.ZERO).toMillis();
        long executionTime =
                queryCompletedEvent.getStatistics().getExecutionTime().orElse(Duration.ZERO).toMillis();
        long peakUserMemoryBytes = queryCompletedEvent.getStatistics().getPeakUserMemoryBytes();
        long peakTaskUserMemory = queryCompletedEvent.getStatistics().getPeakTaskUserMemory();
        long peakTaskTotalMemory = queryCompletedEvent.getStatistics().getPeakTaskTotalMemory();
        long physicalInputBytes = queryCompletedEvent.getStatistics().getPhysicalInputBytes();
        long physicalInputRows = queryCompletedEvent.getStatistics().getPhysicalInputRows();
        String principal = queryCompletedEvent.getContext().getPrincipal().orElse("").replaceAll("\\n","");
        String remoteClientAddress = queryCompletedEvent.getContext().getRemoteClientAddress().orElse("").replaceAll("\\n","");
        String source = queryCompletedEvent.getContext().getSource().orElse("").replaceAll("\\n","");
        String catalog = queryCompletedEvent.getContext().getCatalog().orElse("").replaceAll("\\n","");
        String schema = queryCompletedEvent.getContext().getSchema().orElse("").replaceAll("\\n","");

        String tables = JSON.toJSONString(queryCompletedEvent.getMetadata().getTables());
        String cpuTimeDistribution =
                JSON.toJSONString(queryCompletedEvent.getStatistics().getCpuTimeDistribution());
        String resourceGroupId = JSON.toJSONString(
                queryCompletedEvent.getContext().getResourceGroupId().orElse(new ResourceGroupId("None"))
                        .getSegments());
        String inputs = JSON.toJSONString(queryCompletedEvent.getIoMetadata().getInputs());
        String outputs = JSON.toJSONString(queryCompletedEvent.getIoMetadata().getOutput().orElse(null));
        String warnings = JSON.toJSONString(queryCompletedEvent.getWarnings());

        String createTime = formatter.format(Date.from(queryCompletedEvent.getCreateTime()));
        String endTime = formatter.format(Date.from(queryCompletedEvent.getEndTime()));
        String startTime = formatter.format(Date.from(queryCompletedEvent.getExecutionStartTime()));
        String executionStartTime = formatter.format(Date.from(queryCompletedEvent.getExecutionStartTime()));
        Connection conn = null;
        PreparedStatement ps = null;

        String isFile = config.get("is.database").toUpperCase();
        if (isFile.equals("TRUE")) {
            try {
                conn = hikariDataSource.getConnection();
                //insert into query info table
                String queryInfoSql =
                        "INSERT INTO dr_presto.query_info (queryId, querySql, queryState, queryUser, createTime, endTime, startTime, analysisTime, cpuTime, queuedTime, wallTime, completedSplits, cumulativeMemory, outputBytes, outputRows, totalBytes, totalRows, writtenBytes, writtenRows, transactionId, uri, plan, payload, waitingTime, executionTime, peakUserMemoryBytes, peakTaskUserMemory, peakTaskTotalMemory, physicalInputBytes, physicalInputRows, principal, remoteClientAddress, source, `catalog`, `schema`, executionStartTime, tables, cpuTimeDistribution, resourceGroupId, inputs, outputs, warnings) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                conn.setAutoCommit(false);
                ps = conn.prepareStatement(queryInfoSql);
                ps.setString(1, queryId);
                ps.setString(2, querySql);
                ps.setString(3, queryState);
                ps.setString(4, queryUser);
                ps.setString(5, createTime);
                ps.setString(6, endTime);
                ps.setString(7, startTime);
                ps.setLong(8, analysisTime);
                ps.setLong(9, cpuTime);
                ps.setLong(10, queuedTime);
                ps.setLong(11, wallTime);
                ps.setInt(12, completedSplits);
                ps.setDouble(13, cumulativeMemory);
                ps.setLong(14, outputBytes);
                ps.setLong(15, outputRows);
                ps.setLong(16, totalBytes);
                ps.setLong(17, totalRows);
                ps.setLong(18, writtenBytes);
                ps.setLong(19, writtenRows);
                ps.setString(20, transactionId);
                ps.setString(21, uri);
                ps.setString(22, plan);
                ps.setString(23, payload);
                ps.setLong(24, waitingTime);
                ps.setLong(25, executionTime);
                ps.setLong(26, peakUserMemoryBytes);
                ps.setLong(27, peakTaskUserMemory);
                ps.setLong(28, peakTaskTotalMemory);
                ps.setLong(29, physicalInputBytes);
                ps.setLong(30, physicalInputRows);
                ps.setString(31, principal);
                ps.setString(32, remoteClientAddress);
                ps.setString(33, source);
                ps.setString(34, catalog);
                ps.setString(35, schema);
                ps.setString(36, executionStartTime);
                ps.setString(37, tables);
                ps.setString(38, cpuTimeDistribution);
                ps.setString(39, resourceGroupId);
                ps.setString(40, inputs);
                ps.setString(41, outputs);
                ps.setString(42, warnings);

                int i = ps.executeUpdate();
                conn.commit();
                if (i != 0) {
                    System.out.println(queryId + " 记录插入query info table成功！");
                } else {
                    System.out.println(queryId + " 插入query info tables失败！");
                }
                ps.close();
            } catch (SQLException e) {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException ee) {
                        throw new RuntimeException(ee);
                    }
                }
                throw new RuntimeException(e);
            } finally {
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            System.out.println("================");
            planLogger.info(
                    "queryId:" + queryId + "¶plan:" + plan
            );
            infoLogger.info(
                    "queryId:" + queryId + "¶querySql:" + querySql + "¶queryState:" + queryState +
                            "¶queryUser:" +
                            queryUser + "¶createTime:" + createTime + "¶endTime:" + endTime + "¶startTime:" +
                            startTime +
                            "¶analysisTime:" + analysisTime + "¶cpuTime:" + cpuTime + "¶queuedTime:" + queuedTime +
                            "¶wallTime:" + wallTime + "¶completedSplits:" + completedSplits + "¶cumulativeMemory:" +
                            cumulativeMemory + "¶outputBytes:" + outputBytes + "¶outputRows:" + outputRows +
                            "¶totalBytes:" +
                            totalBytes + "¶totalRows:" + totalRows + "¶writtenBytes:" + writtenBytes + "¶writtenRows:" +
                            writtenRows + "¶transactionId:" + transactionId + "¶uri:" + uri +
                            "¶payload:" +
                            payload + "¶waitingTime:" + waitingTime + "¶executionTime:" + executionTime +
                            "¶peakUserMemoryBytes:" + peakUserMemoryBytes + "¶peakTaskUserMemory:" +
                            peakTaskUserMemory +
                            "¶peakTaskTotalMemory:" + peakTaskTotalMemory + "¶physicalInputBytes:" +
                            physicalInputBytes +
                            "¶physicalInputRows:" + physicalInputRows + "¶principal:" + principal +
                            "¶remoteClientAddress:" +
                            remoteClientAddress + "¶source:" + source + "¶catalog:" + catalog + "¶schema:" + schema +
                            "¶executionStartTime:" + executionStartTime + "¶tables:" + tables +
                            "¶cpuTimeDistribution:" +
                            cpuTimeDistribution + "¶resourceGroupId:" + resourceGroupId + "¶inputs:" + inputs +
                            "¶outputs:" +
                            outputs + "¶warnings:" + warnings);
        }

        queryCompletedEvent.getFailureInfo().ifPresent(queryFailureInfo -> {
                    Connection connF = null;
                    PreparedStatement psF = null;
                    int code = queryFailureInfo.getErrorCode().getCode();
                    String name = queryFailureInfo.getErrorCode().getName();
                    String failureType = queryFailureInfo.getFailureType().orElse("").toUpperCase();
                    String failureHost = queryFailureInfo.getFailureHost().orElse("").toUpperCase();
                    String failureMessage = queryFailureInfo.getFailureMessage().orElse("").toUpperCase();
                    String failureTask = queryFailureInfo.getFailureTask().orElse("").toUpperCase();
                    String failuresJson = queryFailureInfo.getFailuresJson();

                    String isFileF = config.get("is.database").toUpperCase();
                    if (isFileF.equals("TRUE")) {
                        // insert into failed query table
                        String queryFailedSql =
                                "INSERT INTO dr_presto.query_failed (queryId, code, name, failureType, failureHost, failureMessage, failureTask, failuresJson) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
                        try {
                            connF = hikariDataSource.getConnection();
                            connF.setAutoCommit(false);
                            psF = (PreparedStatement) connF.prepareStatement(queryFailedSql);
                            psF.setString(1, queryId);
                            psF.setInt(2, code);
                            psF.setString(3, name);
                            psF.setString(4, failureType);
                            psF.setString(5, failureHost);
                            psF.setString(6, failureMessage);
                            psF.setString(7, failureTask);
                            psF.setString(8, failuresJson);

                            int i = psF.executeUpdate();
                            connF.commit();
                            if (i != 0) {
                                System.out.println(queryId + " 记录插入query failed table成功！");
                            } else {
                                System.out.println(queryId + " 插入query failed tables失败！");
                            }
                            psF.close();
                            connF.close();
                        } catch (SQLException e) {
                            if (psF != null) {
                                try {
                                    psF.close();

                                } catch (SQLException ee) {
                                    throw new RuntimeException(ee);
                                }
                            }
                            throw new RuntimeException(e);
                        } finally {
                            try {
                                connF.close();
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else {
                        failedLogger.info(
                                "queryId:" + queryId + "¶code:" + code + "¶name:" + name +
                                        "¶failureType:" + failureType +
                                        "¶failureHost" + failureHost + "¶failureMessage" + failureMessage + "¶failureTask" +
                                        failureTask + "¶failuresJson" + failuresJson);
                    }
                }
        );

    }

    //    @Override
    //    public void splitCompleted(SplitCompletedEvent splitCompletedEvent) {
    //        Connection conn = null;
    //        PreparedStatement ps = null;
    //        String queryId = splitCompletedEvent.getQueryId();
    //        long createTime = splitCompletedEvent.getCreateTime().toEpochMilli();
    //        long endTime = splitCompletedEvent.getEndTime().orElse(Instant.MIN).toEpochMilli();
    //        String payload = splitCompletedEvent.getPayload();
    //        String stageId = splitCompletedEvent.getStageId();
    //        long startTime = splitCompletedEvent.getStartTime().orElse(Instant.MIN).toEpochMilli();
    //        String taskId = splitCompletedEvent.getTaskId();
    //        long completedDataSizeBytes = splitCompletedEvent.getStatistics().getCompletedDataSizeBytes();
    //        long completedPositions = splitCompletedEvent.getStatistics().getCompletedPositions();
    //        long completedReadTime = splitCompletedEvent.getStatistics().getCompletedReadTime().toMillis();
    //        long cpuTime = splitCompletedEvent.getStatistics().getCpuTime().toMillis();
    //        long queuedTime = splitCompletedEvent.getStatistics().getQueuedTime().toMillis();
    //        long wallTime = splitCompletedEvent.getStatistics().getWallTime().toMillis();
    //        //insert into stage info table
    //
    //        String stageInfoSql =
    //                "INSERT INTO dr_presto.split_info (queryId, createTime, endTime, payload, stageId, startTime, taskId, completedDataSizeBytes, completedPositions, completedReadTime, cpuTime, queuedTime, wallTime) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    //        try {
    //            conn = hikariDataSource.getConnection();
    //            conn.setAutoCommit(false);
    //            ps = conn.prepareStatement(stageInfoSql);
    //            ps.setString(1, queryId);
    //            ps.setLong(2, createTime);
    //            ps.setLong(3, endTime);
    //            ps.setString(4, payload);
    //            ps.setString(5, stageId);
    //            ps.setLong(6, startTime);
    //            ps.setString(7, taskId);
    //            ps.setLong(8, completedDataSizeBytes);
    //            ps.setLong(9, completedPositions);
    //            ps.setLong(10, completedReadTime);
    //            ps.setLong(11, cpuTime);
    //            ps.setLong(12, queuedTime);
    //            ps.setLong(13, wallTime);
    //
    //            int i = ps.executeUpdate();
    //            conn.commit();
    //            if (i != 0) {
    //                System.out.println(queryId + " 记录插入stage info table成功！");
    //            } else {
    //                System.out.println(queryId + " 插入stage info tables失败！");
    //            }
    //            ps.close();
    //        } catch (SQLException e) {
    //            if (ps != null) {
    //                try {
    //                    ps.close();
    //                } catch (SQLException ee) {
    //                    throw new RuntimeException(ee);
    //                }
    //            }
    //            throw new RuntimeException(e);
    //        } finally {
    //            try {
    //                conn.close();
    //            } catch (SQLException e) {
    //                throw new RuntimeException(e);
    //            }
    //        }
    //    }
}
