namespace AotKafka.Native;

/// <summary>
/// Admin operation types used when creating admin options.
/// </summary>
public enum RdKafkaAdminOp
{
    Any = 0,
    CreateTopics = 1,
    DeleteTopics = 2,
    CreatePartitions = 3,
    AlterConfigs = 4,
    DescribeConfigs = 5,
    DeleteRecords = 6,
    DeleteGroups = 7,
    DeleteConsumerGroupOffsets = 8,
    CreateAcls = 9,
    DescribeAcls = 10,
    DeleteAcls = 11,
    ListConsumerGroups = 12,
    DescribeConsumerGroups = 13,
    ListConsumerGroupOffsets = 14
}
