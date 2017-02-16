#### 3.1.7 - 16.02.2017
* Updates packages

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
