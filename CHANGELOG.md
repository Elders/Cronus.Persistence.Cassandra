# [6.3.0-preview.4](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v6.3.0-preview.3...v6.3.0-preview.4) (2021-10-22)


### Bug Fixes

* Updates packages ([d6bec5f](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/d6bec5f0b47351d6ee5d4bc81a46e0a2279ee460))

# [6.3.0-preview.3](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v6.3.0-preview.2...v6.3.0-preview.3) (2021-05-11)


### Bug Fixes

* Fixes AggregateCommits loading ([3de7b23](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/3de7b2318c80ec7c01c68a3abb0956e84a44ba81))

# [6.3.0-preview.2](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v6.3.0-preview.1...v6.3.0-preview.2) (2021-05-11)


### Bug Fixes

* Consolidates release notes ([4114a80](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/4114a803a2a0f037cef0a9f3374e480a4b403fb6))
* Fixes release flow ([94abd29](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/94abd296d122557935ef5b5a78963aa76dbc9e5d))
* Implements new replay option ([03b89b6](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/03b89b632ca92041787843acafe82a279a738c41))
* Updates Cronus ([c7245f2](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/c7245f2b3d4c3188eafa5d92a1b452b1407d2778))

## [6.3.0-preview.1](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v6.2.5-preview.1...v6.2.5-preview.2) (2021-05-11)


### Bug Fixes

* Consolidates release notes ([4114a80](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/4114a803a2a0f037cef0a9f3374e480a4b403fb6))
* Implements new replay option ([03b89b6](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/03b89b632ca92041787843acafe82a279a738c41))
* Updates Cronus ([c7245f2](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/c7245f2b3d4c3188eafa5d92a1b452b1407d2778))

# [6.3.0-preview.1](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v6.2.5-preview.1...v6.3.0-preview.1) (2021-05-11)


### Bug Fixes

* Consolidates release notes ([4114a80](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/4114a803a2a0f037cef0a9f3374e480a4b403fb6))
* Implements new replay option ([03b89b6](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/03b89b632ca92041787843acafe82a279a738c41))
* Updates Cronus ([c7245f2](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/c7245f2b3d4c3188eafa5d92a1b452b1407d2778))

## [6.2.5-preview.2](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v6.2.5-preview.1...v6.2.5-preview.2) (2021-05-11)


### Bug Fixes

* Implements new replay option ([03b89b6](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/03b89b632ca92041787843acafe82a279a738c41))
* Updates Cronus ([c7245f2](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/c7245f2b3d4c3188eafa5d92a1b452b1407d2778))

## [6.2.5-preview.1](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v6.2.4...v6.2.5-preview.1) (2021-05-07)


### Bug Fixes

* Fixes copyright attribute ([779131f](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/779131f70ca04d2384ee6bc46fd7fd1da684ab92))
* Project Cleanup ([55b62f6](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/55b62f67fb3e7538b4748570b380d73a372b4a40))
* Removes gitversion ([42355cf](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/42355cfaf05e45c338dcd214bd0b6f25a29bbee6))
* Updates Cronus ([d64d259](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/d64d2591bde4be4026237c4a5e89430d3b036173))


#### 6.3.0-beta0002 - 19.03.2021
* Improves cassandra statements

#### 6.3.0-beta0001 - 16.03.2021
* Updates Cronus

#### 6.2.4 - 05.10.2020
* Fixes Cassandra paging

#### 6.2.3 - 05.10.2020
* Fixes Cassandra paging

#### 6.2.2 - 05.10.2020
* Fixes Cassandra paging

#### 6.2.1 - 02.10.2020
* Allows to override table name strategy explicitly

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