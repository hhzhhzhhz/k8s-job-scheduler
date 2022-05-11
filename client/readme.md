#注意事项
 * cronjob 运行的过程产生once_job, 因此在处理状态回调把cronjob 产生的job 当做once_job处理, 只有cronjob 本身的创建、删除、失败才当做cronjob类型事件