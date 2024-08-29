using System;
using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.EventStore;

namespace Elders.Cronus.Persistence.Cassandra
{
    internal class AggregateCommitBlock
    {
        private readonly IBlobId id;
        private int revision;
        private long timestamp;
        public const int PublicEventsOffset = 5;

        public AggregateCommitBlock(IBlobId id)
        {
            this.id = id;
            revision = 1;
            Events = new List<IEvent>();
            PublicEvents = new List<IPublicEvent>();
            aggregateCommits = new List<AggregateCommit>();
        }

        private int GetNextExpectedEventPosition() => Events.Count;

        private int GetNextExpectedPublicEventPosition() => Events.Count + PublicEventsOffset + PublicEvents.Count;

        private List<IEvent> Events { get; set; }

        private List<IPublicEvent> PublicEvents { get; set; }

        List<AggregateCommit> aggregateCommits;

        internal void AppendBlock(int revision, int position, IMessage data, long timestamp)
        {
            if (this.timestamp == 0)
                this.timestamp = timestamp;

            if (this.revision == revision)
            {
                AttachDataAtPosition(data, position);
            }
            else if (this.revision < revision)
            {
                var aggregateCommit = new AggregateCommit(id.RawId, this.revision, Events.ToList(), PublicEvents.ToList(), this.timestamp);
                aggregateCommits.Add(aggregateCommit);

                Events.Clear();
                PublicEvents.Clear();

                this.revision = revision;
                this.timestamp = timestamp;
                AttachDataAtPosition(data, position);
            }
        }

        private void AttachDataAtPosition(IMessage data, int position)
        {
            if (GetNextExpectedEventPosition() == position) // If the event we want to attach is IEvent (not public) and it is the first one 
                Events.Add((IEvent)data);
            else if (GetNextExpectedPublicEventPosition() >= position)
                PublicEvents.Add((IPublicEvent)data);
            else
                throw new NotSupportedException("How?!?!?");
        }

        public List<AggregateCommit> Complete()
        {
            if (Events.Any())
            {
                // Appends the everything we have in memory to the final result
                var aggregateCommit = new AggregateCommit(id.RawId, revision, Events.ToList(), PublicEvents.ToList(), timestamp);
                aggregateCommits.Add(aggregateCommit);
            }

            return aggregateCommits;
        }

        internal class CassandraRawId : IBlobId
        {
            public CassandraRawId(ReadOnlyMemory<byte> rawId)
            {
                RawId = rawId;
            }

            public ReadOnlyMemory<byte> RawId { get; private set; }
        }
    }

}
