# once_job
DROP TABLE IF EXISTS `once_job`;
CREATE TABLE `once_job` (
    `id` int NOT NULL AUTO_INCREMENT COMMENT 'Id自增',
    `job_id` varchar(50) NOT NULL COMMENT '任务Id',
    `job_name` varchar(128) NOT NULL COMMENT '任务名称',
    `namespace` varchar(128) NOT NULL COMMENT '命名空间',
    `image` varchar(200) NOT NULL COMMENT '运行镜像',
    `command` text NOT NULL COMMENT '运行命令',
    `job_exec_time` int NOT NULL COMMENT '任务时间',
    `metadata` text NOT NULL COMMENT '自定义功能: labels,annotation, 回调接口等等信息',
    `create_time` int NOT NULL COMMENT '创建时间',
    `update_time` int NOT NULL COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY job_id (job_id),
    UNIQUE KEY job_name (job_name)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8 COMMENT='单次任务表';

# cron_job
DROP TABLE IF EXISTS `cron_job`;
CREATE TABLE `cron_job` (
    `id` int NOT NULL AUTO_INCREMENT COMMENT 'Id自增',
    `job_id` varchar(50) NOT NULL COMMENT '任务Id',
    `job_name` varchar(128) NOT NULL COMMENT '任务名称',
    `schedule` varchar(128) NOT NULL COMMENT '执行周期',
    `namespace` varchar(128) NOT NULL COMMENT '命名空间',
    `image` varchar(200) NOT NULL COMMENT '运行镜像',
    `command` text NOT NULL COMMENT '运行命令',
    `metadata` text NOT NULL COMMENT '自定义功能: labels,annotation, 回调接口等等信息',
    `create_time` int NOT NULL COMMENT '创建时间',
    `update_time` int NOT NULL COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY job_id (job_id),
    UNIQUE KEY job_name (job_name)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT='周期任务表';

# job_status
DROP TABLE IF EXISTS `job_status`;
CREATE TABLE `job_status`(
    `id` int NOT NULL AUTO_INCREMENT COMMENT 'ID',
    `job_id` varchar(128) NOT NULL COMMENT '任务id',
    `job_name` varchar(128) NOT NULL COMMENT '任务名称',
    `job_type` int NOT NULL DEFAULT 0 COMMENT '任务类型(0:一次性任务，1: 定时任务)',
    `job_state` int NOT NULL DEFAULT 0 COMMENT '任务状态,-1:发送, 0:创建成功,1:失败,2:运行成功,3:任务删除',
    `job_times` int NOT NULL DEFAULT '0' COMMENT '任务运行次数',
    `job_success_times` int NOT NULL DEFAULT '0' COMMENT '任务成功次数',
    `job_exec_information` text COMMENT '执行信息',
    `create_time` int NOT NULL COMMENT '创建时间',
    `update_time` int NOT NULL COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务状态';

# job_logs
DROP TABLE IF EXISTS `job_logs`;
CREATE TABLE `job_logs` (
    `id` int NOT NULL AUTO_INCREMENT COMMENT 'Id自增',
    `job_id` varchar(128) NOT NULL COMMENT '任务id',
    `pod_name` varchar(128) NOT NULL COMMENT 'pod名称',
    `run_logs` mediumblob NOT NULL COMMENT 'job 执行日志',
    `create_time` int NOT NULL COMMENT '创建时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT='任务日志记录表';
