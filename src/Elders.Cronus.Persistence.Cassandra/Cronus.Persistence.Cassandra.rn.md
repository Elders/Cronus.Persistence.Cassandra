#### 6.2.0 - 01.10.2020
* Overrides default inmemory configurations
* Adds pagination when enumerating indices

#### 6.1.1 - 24.09.2020
* Starts using GetSession from CassandraProvider instead of local copies for connections
* Updates Cronus package to 6.1.1

#### 6.1.0 - 24.08.2020
* Updates packages

#### 6.0.0 - 16.04.2020
* Support for async loading of aggregate commits
* Rework the CassandraProvideroptions to use options pattern
* Adds manual paging of the event store
* Targets netcoreapp3.1
* Configures AppVeyor

#### 5.3.1 - 02.05.2019
* Fixes a memory leak due to TCP Socket management

#### 5.3.0 - 05.02.2019
* Updated Cronus to 5.3.0
* Replaced CassandraProvider to ICassandraProvider in CassandraEventStoreSchema so that it can be implemented in Client Projects

#### 5.2.0 - 10.01.2019
* Abstracted CassandraEventStoreSchema for integration testing purposes
* Marks CronusCassandraEventStoreStartup with [CronusStartup(Bootstraps.ExternalResource)]
* Adds CronusCassandraEventStoreStartup for creating the initial database

#### 5.1.0 - 10.12.2018
* Updates to DNC 2.2

#### 5.0.0 - 29.11.2018
* Various fixes and improvements
* Fixes EventStorePlayer query
* Adds support for AggregateCommitRaw in the CassandraEventStore
* Adds EventStore support for generic types
* Implements the generic interfaces for EventStore and EventStorePlayer
* Uses `BoundedContext` instead of IConfiguration for capturing the cronus_boundedcontext value_
* Fixes the CassandraEventStoreSchema discovery registration
* Adds event store index storage
* Removes ISettingsBuilder configurations

#### 4.1.1 - 28.03.2018
* Adds validation check

#### 4.1.0 - 22.03.2018
* Updates Cronus
* Multitenancy support

#### 4.0.3 - 28.02.2018
* Updates packages

#### 4.0.2 - 20.02.2018
* Updates packages

#### 4.0.1 - 20.02.2018
* Targets netstandard2.0;net45;net451;net452;net46;net461;net462

#### 4.0.0 - 12.02.2018
* This release uses the official netstandard 2.0
* BREAKING: configuration via app/web.config section is not supported anymore. Use Pandora.

#### 3.3.1 - 24.07.2017
* Change the default retry policy to the Cassandra DefaultRetryPolicy.
* Cassandra WriteTimeoutException is not considered as an error we only log warning about it.

#### 3.3.0 - 26.04.2017
* Add support for Cassandra cluster

#### 3.3.0-beta0004 - 19.04.2017
* Fix the setting of consistency level

#### 3.3.0-beta0003 - 18.04.2017
* Add settings for read and write consistency level

#### 3.3.0-beta0002 - 13.04.2017
* Improve the configuration API

#### 3.3.0-beta0001 - 15.03.2017
* Add settings for replication strategies.

#### 3.2.0 - 15.03.2017
* Command store

#### 3.1.6 - 15.06.2016
* Add the ability to create TablePerBoundedContext with BoundedContextName directly.

#### 3.1.5 - 19.03.2016
* Update packages

#### 3.1.4 - 03.07.2015
* Update packages

#### 3.1.3 - 03.07.2015
* Update packages

#### 3.1.2 - 02.07.2015
* Update DomainModeling

#### 3.1.1 - 02.07.2015
* Update DomainModeling

#### 3.1.0 - 02.07.2015
* Update DomainModeling

#### 3.0.0 - 30.06.2015
* Change the schema of the event storage. Now the revision is part of the cluster key.

#### 2.1.0 - 25.06.2015
* Introduce EventStoreNoHintedHandOff retry policy. We will handle the retry instead of Cassandra

#### 2.0.1 - 16.05.2015
* Build for Cronus 2.*

#### 2.0.0 - 16.05.2015
* Build for Cronus 2.*

#### 1.2.11 - 21.04.2015
* Update packages

#### 1.2.10 - 04.16.2015
* Update packages

#### 1.2.9 - 04.16.2015
* Deprecate writing in "*player" table.
* Loading the events for Replay are now unordered from the "ES" table

#### 1.2.8 - 04.04.2015
* Replaying events now returns the entire AggregateCommit

#### 1.2.7 - 03.04.2015
* Add log error when event deserialization fails while replaying the events

#### 1.2.6 - 03.04.2015
* Fixed issue with the initial date when replaying events

#### 1.2.5 - 03.04.2015
* Added the IEventStorePlayer to the Container

#### 1.2.4 - 13.03.2015
* Remove AggregateRepository init

#### 1.2.3 - 12.03.2015
* Update Cronus package

#### 1.2.2 - 07.02.2015
* Update Cassandra package

#### 1.2.1 - 13.01.2015
* Update Cronus package

#### 1.2.0 - 16.12.2014
* Improved version with many changes towards more natural usage

#### 1.0.1 - 11.09.2014
* Fix bug with nuget package release

#### 1.0.0 - 10.09.2014
* Moved from the Cronus repository
