namespace AotKafka.Native;

/// <summary>
/// Event types returned from librdkafka queues.
/// </summary>
public enum RdKafkaEventType
{
    None = 0,
    Error = 0x8,
    CreateTopicsResult = 100,
    DeleteTopicsResult = 101,
    CreatePartitionsResult = 102,
    AlterConfigsResult = 103,
    DescribeConfigsResult = 104,
    DeleteRecordsResult = 105,
    CreateAclsResult = 106,
    DescribeAclsResult = 107,
    DeleteAclsResult = 108,
    ListConsumerGroupsResult = 109,
    DescribeConsumerGroupsResult = 110,
    DeleteGroupsResult = 111,
    DeleteConsumerGroupOffsetsResult = 112,
    ListConsumerGroupOffsetsResult = 113,
    AlterConsumerGroupOffsetsResult = 114
}
