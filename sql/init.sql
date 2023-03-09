CREATE DATABASE `dr_presto` ;


-- dr_presto.query_info definition

CREATE TABLE `query_info` (
                              `queryId` varchar(100) DEFAULT NULL,
                              `querySql` longtext,
                              `queryState` varchar(100) DEFAULT NULL,
                              `queryUser` varchar(100) DEFAULT NULL,
                              `createTime` bigint DEFAULT NULL,
                              `endTime` bigint DEFAULT NULL,
                              `startTime` bigint DEFAULT NULL,
                              `analysisTime` bigint DEFAULT NULL,
                              `cpuTime` bigint DEFAULT NULL,
                              `queuedTime` bigint DEFAULT NULL,
                              `wallTime` bigint DEFAULT NULL,
                              `completedSplits` int DEFAULT NULL,
                              `cumulativeMemory` double DEFAULT NULL,
                              `outputBytes` bigint DEFAULT NULL,
                              `outputRows` bigint DEFAULT NULL,
                              `totalBytes` bigint DEFAULT NULL,
                              `totalRows` bigint DEFAULT NULL,
                              `writtenBytes` bigint DEFAULT NULL,
                              `writtenRows` bigint DEFAULT NULL,
                              `transactionId` varchar(100) DEFAULT NULL,
                              `uri` varchar(100) DEFAULT NULL,
                              `plan` longtext CHARACTER SET utf8 COLLATE utf8_general_ci,
                              `payload` longtext,
                              `waitingTime` bigint DEFAULT NULL,
                              `executionTime` bigint DEFAULT NULL,
                              `peakUserMemoryBytes` bigint DEFAULT NULL,
                              `peakTaskUserMemory` bigint DEFAULT NULL,
                              `peakTaskTotalMemory` bigint DEFAULT NULL,
                              `physicalInputBytes` bigint DEFAULT NULL,
                              `physicalInputRows` bigint DEFAULT NULL,
                              `principal` varchar(100) DEFAULT NULL,
                              `remoteClientAddress` varchar(100) DEFAULT NULL,
                              `source` varchar(100) DEFAULT NULL,
                              `catalog` varchar(100) DEFAULT NULL,
                              `schema` varchar(100) DEFAULT NULL,
                              `executionStartTime` bigint DEFAULT NULL,
                              `tables` text,
                              `cpuTimeDistribution` text,
                              `resourceGroupId` text,
                              `inputs` text,
                              `outputs` text,
                              `warnings` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;


-- dr_presto.query_failed definition

CREATE TABLE `query_failed` (
                                `queryId` varchar(100) DEFAULT NULL,
                                `code` int DEFAULT NULL,
                                `name` varchar(100) DEFAULT NULL,
                                `failureType` varchar(100) DEFAULT NULL,
                                `failureHost` varchar(100) DEFAULT NULL,
                                `failureMessage` varchar(100) DEFAULT NULL,
                                `failureTask` varchar(100) DEFAULT NULL,
                                `failuresJson` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- dr_presto.split_info definition

CREATE TABLE `split_info` (
                              `queryId` varchar(100) DEFAULT NULL,
                              `createTime` bigint DEFAULT NULL,
                              `endTime` bigint DEFAULT NULL,
                              `payload` text,
                              `stageId` varchar(100) DEFAULT NULL,
                              `startTime` bigint DEFAULT NULL,
                              `taskId` varchar(100) DEFAULT NULL,
                              `completedDataSizeBytes` bigint DEFAULT NULL,
                              `completedPositions` bigint DEFAULT NULL,
                              `completedReadTime` bigint DEFAULT NULL,
                              `cpuTime` bigint DEFAULT NULL,
                              `queuedTime` bigint DEFAULT NULL,
                              `wallTime` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;