using Confluent.Kafka;
using Confluent.Kafka.Admin;
using OrderFlow.Triage.Models;

namespace OrderFlow.Triage;

/// <summary>
/// Reads committed offsets and high watermarks so triage can tell "the consumer is
/// down" apart from "the consumer is behind" — the two look identical from the
/// database alone.
/// </summary>
internal sealed class KafkaLagReader(TriageOptions options)
{
    private static readonly string[] ConsumerGroups =
    [
        "inventory-service",
        "order-api",
        "notification-worker",
    ];

    public Task<ConsumerLagSnapshot> ReadAsync(CancellationToken cancellationToken = default)
    {
        // The Confluent client is synchronous under the hood; keep the blocking work
        // off the caller's thread rather than pretending it is async.
        return Task.Run(() => Read(cancellationToken), cancellationToken);
    }

    private ConsumerLagSnapshot Read(CancellationToken cancellationToken)
    {
        var groups = new List<ConsumerGroupLag>();

        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = options.KafkaBootstrapServers,
        }).Build();

        // A consumer is only needed for QueryWatermarkOffsets; it never joins the group.
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
        {
            BootstrapServers = options.KafkaBootstrapServers,
            GroupId = "orderflow-triage-watermark-probe",
            EnableAutoCommit = false,
        }).Build();

        List<TopicPartition> partitions;
        try
        {
            partitions = DiscoverPartitions(admin);
        }
        catch (KafkaException ex)
        {
            return new ConsumerLagSnapshot(ConsumerGroups
                .Select(g => new ConsumerGroupLag(g, false, 0, Array.Empty<PartitionLag>(),
                    $"Kafka unreachable at {options.KafkaBootstrapServers}: {ex.Message}"))
                .ToList());
        }

        var watermarks = new Dictionary<TopicPartition, WatermarkOffsets>();
        foreach (var partition in partitions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                watermarks[partition] = consumer.QueryWatermarkOffsets(partition, options.KafkaTimeout);
            }
            catch (KafkaException)
            {
                // Leave it out; the partition simply won't appear in the lag report.
            }
        }

        foreach (var group in ConsumerGroups)
        {
            cancellationToken.ThrowIfCancellationRequested();
            groups.Add(ReadGroup(admin, group, partitions, watermarks));
        }

        return new ConsumerLagSnapshot(groups);
    }

    private List<TopicPartition> DiscoverPartitions(IAdminClient admin)
    {
        var metadata = admin.GetMetadata(options.KafkaTimeout);
        return metadata.Topics
            .Where(t => TriageTools.Topics.Contains(t.Topic))
            .SelectMany(t => t.Partitions.Select(p => new TopicPartition(t.Topic, p.PartitionId)))
            .ToList();
    }

    private ConsumerGroupLag ReadGroup(
        IAdminClient admin,
        string groupId,
        List<TopicPartition> partitions,
        IReadOnlyDictionary<TopicPartition, WatermarkOffsets> watermarks)
    {
        List<TopicPartitionOffsetError> committed;
        try
        {
            var results = admin.ListConsumerGroupOffsetsAsync(
                [new ConsumerGroupTopicPartitions(groupId, partitions)],
                new ListConsumerGroupOffsetsOptions { RequestTimeout = options.KafkaTimeout })
                .GetAwaiter().GetResult();

            committed = results.SelectMany(r => r.Partitions).ToList();
        }
        catch (Exception ex)
        {
            return new ConsumerGroupLag(groupId, false, 0, Array.Empty<PartitionLag>(),
                $"Could not read offsets: {ex.Message}");
        }

        var lags = new List<PartitionLag>();
        foreach (var entry in committed)
        {
            if (!watermarks.TryGetValue(entry.TopicPartition, out var watermark)) continue;

            // Offset.Unset (-1001) means this group has never committed here — which is
            // normal for a topic the group does not subscribe to, so it is skipped
            // rather than reported as lag equal to the whole partition.
            if (entry.Offset == Offset.Unset) continue;

            var lag = Math.Max(0, watermark.High.Value - entry.Offset.Value);
            lags.Add(new PartitionLag(
                Topic: entry.Topic,
                Partition: entry.Partition.Value,
                CommittedOffset: entry.Offset.Value,
                HighWatermark: watermark.High.Value,
                Lag: lag));
        }

        return lags.Count == 0
            ? new ConsumerGroupLag(groupId, false, 0, Array.Empty<PartitionLag>(),
                "No committed offsets — the group has not consumed anything yet, or has never run.")
            : new ConsumerGroupLag(
                GroupId: groupId,
                Found: true,
                TotalLag: lags.Sum(x => x.Lag),
                Partitions: lags.OrderBy(x => x.Topic).ThenBy(x => x.Partition).ToList());
    }
}
