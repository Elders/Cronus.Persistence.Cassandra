## [9.1.2](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.1.1...v9.1.2) (2023-10-06)


### Bug Fixes

* Handles 'null' case when deserializing AggregateCommit in CassandraEventStorePlayer_v8 ([b5aa4ed](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/b5aa4edb645cbaf667bce7033b32b8fe2a7f8a34))

## [9.1.1](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.1.0...v9.1.1) (2023-09-29)


### Bug Fixes

* Fixes the CronusMigrator registry to the IServiceCollection ([5176683](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/5176683760616b022f1e6fe95ac74d34165e83e2))

# [9.1.0](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.2...v9.1.0) (2023-09-25)


### Features

* Adds migration logic from v8 to v9 ([0ec6eaa](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/0ec6eaab088ad62bee9086b743b8a7088ab10b42))

## [9.0.2](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.1...v9.0.2) (2023-09-18)


### Bug Fixes

* Fixes the bound statement when appending new AggregateEventRaw ([b9a05a5](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/b9a05a5e050630003785e980898f2e515da34f2e))

## [9.0.1](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0...v9.0.1) (2023-08-25)


### Bug Fixes

* Fixes queries for MessageCounter ([d48b46c](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/d48b46c6a8c95f9c70f77c30fffddc3e5f0e8cd4))
* Properly manages cluster instances for Cassandra ([b44021e](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/b44021ee49d386e7f98d2019b67d7400973d461f))

# [9.0.0](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v8.0.5...v9.0.0) (2023-08-22)


### Bug Fixes

* Adds error  logs when replaying ([84a180b](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/84a180bbbaae3b37dc1901247000d22e893ebe8e))
* Adds error handling when counting messages ([0e697cc](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/0e697cc1a308ca0616e41a0caca1f2fd7c26789a))
* Adds method to load PublicEvents when we do replay ([a9afdda](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/a9afdda5fe10fe5e4701b27869366228a8fa2bc4))
* Code cleanup ([636495b](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/636495bf14866030ea30805ecdb736e4aaf163a5))
* Code cleanup ([f68555d](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/f68555d8b8ac6d6887cab0f9d638d29c770c9e1c))
* Code cleanup ([3c25d12](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/3c25d1206b2bfe13028036e870181c114601c52a))
* Code refactoring ([6e45892](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/6e45892f804c9ca5a9d06158a3b059dd8453d88a))
* Configures pagination when enumerating the event store ([5abd74e](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/5abd74e0937bf38a2c6c50ab3af83403bfa25996))
* Consistency level fix ([907a1d9](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/907a1d95afea37c408defaa472a084eaeeb5e73d))
* Executes all statements in a BatchStatement when doing append ([ab74902](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/ab74902866d6088d382665d76cd58ebb0cafb275))
* Fix solution after a huge merge ([859e829](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/859e82994c2423e044122132c58464ade0315185))
* Fixes count query ([4568998](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/45689984b8b8778e977201f52718884a882a3f27))
* Fixes flow and logs ([360ccbf](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/360ccbffea1de03920473954b97567b5f5ffbd64))
* Fixes message counter read ([5c58fc6](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/5c58fc66c3243dbd080c71be0f2e2557d5c896d0))
* fixes pagination when loading AR Commits ([00c641c](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/00c641c216336a1d980f1879818ee819c6f3cdc7))
* Implements public event replay with IAsyncEnumerable ([525157a](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/525157ad4948a722b328a69b9984a0ce27e539cf))
* Improves performance ([689af69](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/689af69cbaf3727a1fb11a9501a5157301d10ca4))
* Remove all 'new' occurrences ([193deeb](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/193deebdd8a4c2ed03825a8f8f36b046e94e82d1))
* Resurrects CassandraEventStorePlayer_v8 ([73b6e31](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/73b6e31739a0a6f68e19eb3def2ebacf5a45f982))
* Try fixing deserialize error ([d1a4e2e](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/d1a4e2ee01cbfeed0e63851be068d74aeab5a602))
* Try fixing deserialize error ([d13ca6c](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/d13ca6c55dbc7d815afc3ef2f2d35b1dc0eb0227))
* Try fixing deserialize problem ([71d0552](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/71d0552fe48a56d31272258a97f5f4f714c95441))
* Try using high level of timeouts when counting ([f6fa2d3](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/f6fa2d3b55bbc483e4258ef4f97f6ee470fcfc73))
* Try using high level of timeouts when counting ([261f471](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/261f471e819c82247c3ac9e10a898ab2d8457daa))
* Try using high level of timeouts when counting ([56f4498](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/56f4498dd5b3c69d2e45571546ab0a5b48d23858))
* Updates Cronus ([1716502](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/171650285fcebb94768e77ad03e6276a01c99bf7))
* Updates Cronus ([5283170](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/52831703ee6d3af3701c4200030867e154026bdb))
* Updates packages ([558bfef](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/558bfefdcd27d8937a221e0dd012bb2d3f8ef3b7))
* Updates Source link ([21e9c14](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/21e9c141163a743f881ffe5a29e0ea94cf6f06fa))


### Features

* Adds the ability to load ARs with paging ([817eb83](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/817eb83c805690a9975a301a6bbec9fa108d4d05))
* Allow configuring MaxRequestsPerConnection ([6cecc57](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/6cecc575c7e1222064f4415a33c60d99ed6ffce3))
* Control MaxDegreeOfParallelism via parameter ([b08b0db](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/b08b0db037eaeca93a723126c8472136e856b231))
* Delete AggregateEventRaw from the event store ([26bdbba](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/26bdbbad3fccf619b45e27d9ad49b09ff3534efa))

# [9.0.0-preview.32](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.31...v9.0.0-preview.32) (2023-08-07)

# [9.0.0-preview.31](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.30...v9.0.0-preview.31) (2023-07-31)

# [9.0.0-preview.30](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.29...v9.0.0-preview.30) (2023-07-20)

# [9.0.0-preview.29](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.28...v9.0.0-preview.29) (2023-06-27)


### Features

* Adds the ability to load ARs with paging ([817eb83](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/817eb83c805690a9975a301a6bbec9fa108d4d05))

# [9.0.0-preview.28](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.27...v9.0.0-preview.28) (2023-06-06)


### Features

* Control MaxDegreeOfParallelism via parameter ([b08b0db](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/b08b0db037eaeca93a723126c8472136e856b231))

# [9.0.0-preview.27](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.26...v9.0.0-preview.27) (2023-05-09)


### Bug Fixes

* Updates packages ([558bfef](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/558bfefdcd27d8937a221e0dd012bb2d3f8ef3b7))

# [9.0.0-preview.26](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.25...v9.0.0-preview.26) (2023-05-09)


### Features

* Allow configuring MaxRequestsPerConnection ([6cecc57](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/6cecc575c7e1222064f4415a33c60d99ed6ffce3))

# [9.0.0-preview.25](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.24...v9.0.0-preview.25) (2023-05-05)


### Bug Fixes

* Executes all statements in a BatchStatement when doing append ([ab74902](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/ab74902866d6088d382665d76cd58ebb0cafb275))

# [9.0.0-preview.24](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.23...v9.0.0-preview.24) (2023-03-29)


### Bug Fixes

* Configures pagination when enumerating the event store ([5abd74e](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/5abd74e0937bf38a2c6c50ab3af83403bfa25996))

# [9.0.0-preview.23](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.22...v9.0.0-preview.23) (2023-03-29)


### Bug Fixes

* Fixes flow and logs ([360ccbf](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/360ccbffea1de03920473954b97567b5f5ffbd64))

# [9.0.0-preview.22](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.21...v9.0.0-preview.22) (2023-03-29)


### Bug Fixes

* Adds error  logs when replaying ([84a180b](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/84a180bbbaae3b37dc1901247000d22e893ebe8e))

# [9.0.0-preview.21](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.20...v9.0.0-preview.21) (2023-02-26)


### Bug Fixes

* Try using high level of timeouts when counting ([f6fa2d3](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/f6fa2d3b55bbc483e4258ef4f97f6ee470fcfc73))

# [9.0.0-preview.20](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.19...v9.0.0-preview.20) (2023-02-26)


### Bug Fixes

* Try using high level of timeouts when counting ([261f471](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/261f471e819c82247c3ac9e10a898ab2d8457daa))

# [9.0.0-preview.19](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.18...v9.0.0-preview.19) (2023-02-26)


### Bug Fixes

* Try using high level of timeouts when counting ([56f4498](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/56f4498dd5b3c69d2e45571546ab0a5b48d23858))

# [9.0.0-preview.18](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.17...v9.0.0-preview.18) (2023-02-08)


### Features

* Delete AggregateEventRaw from the event store ([26bdbba](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/26bdbbad3fccf619b45e27d9ad49b09ff3534efa))

# [9.0.0-preview.17](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.16...v9.0.0-preview.17) (2023-01-19)


### Bug Fixes

* Consistency level fix ([907a1d9](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/907a1d95afea37c408defaa472a084eaeeb5e73d))

# [9.0.0-preview.16](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.15...v9.0.0-preview.16) (2023-01-19)


### Bug Fixes

* Adds error handling when counting messages ([0e697cc](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/0e697cc1a308ca0616e41a0caca1f2fd7c26789a))

# [9.0.0-preview.15](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.14...v9.0.0-preview.15) (2023-01-19)

# [9.0.0-preview.14](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.13...v9.0.0-preview.14) (2022-12-15)


### Bug Fixes

* fixes pagination when loading AR Commits ([00c641c](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/00c641c216336a1d980f1879818ee819c6f3cdc7))

# [9.0.0-preview.13](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.12...v9.0.0-preview.13) (2022-11-24)


### Bug Fixes

* Implements public event replay with IAsyncEnumerable ([525157a](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/525157ad4948a722b328a69b9984a0ce27e539cf))

# [9.0.0-preview.12](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.11...v9.0.0-preview.12) (2022-10-20)


### Bug Fixes

* Updates Cronus ([1716502](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/171650285fcebb94768e77ad03e6276a01c99bf7))

# [9.0.0-preview.11](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.10...v9.0.0-preview.11) (2022-10-19)


### Bug Fixes

* Adds method to load PublicEvents when we do replay ([a9afdda](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/a9afdda5fe10fe5e4701b27869366228a8fa2bc4))
* Code cleanup ([636495b](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/636495bf14866030ea30805ecdb736e4aaf163a5))

# [9.0.0-preview.10](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.9...v9.0.0-preview.10) (2022-10-06)


### Bug Fixes

* Fixes count query ([4568998](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/45689984b8b8778e977201f52718884a882a3f27))
* Updates Cronus ([5283170](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/52831703ee6d3af3701c4200030867e154026bdb))

# [9.0.0-preview.9](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.8...v9.0.0-preview.9) (2022-10-05)


### Bug Fixes

* Code cleanup ([f68555d](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/f68555d8b8ac6d6887cab0f9d638d29c770c9e1c))
* Improves performance ([689af69](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/689af69cbaf3727a1fb11a9501a5157301d10ca4))

# [9.0.0-preview.8](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.7...v9.0.0-preview.8) (2022-10-04)


### Bug Fixes

* Code cleanup ([3c25d12](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/3c25d1206b2bfe13028036e870181c114601c52a))
* Updates Source link ([21e9c14](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/21e9c141163a743f881ffe5a29e0ea94cf6f06fa))

# [9.0.0-preview.7](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.6...v9.0.0-preview.7) (2022-10-04)


### Bug Fixes

* Try fixing deserialize error ([d1a4e2e](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/d1a4e2ee01cbfeed0e63851be068d74aeab5a602))

# [9.0.0-preview.6](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.5...v9.0.0-preview.6) (2022-10-04)


### Bug Fixes

* Try fixing deserialize error ([d13ca6c](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/d13ca6c55dbc7d815afc3ef2f2d35b1dc0eb0227))

# [9.0.0-preview.5](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.4...v9.0.0-preview.5) (2022-10-04)


### Bug Fixes

* Try fixing deserialize problem ([71d0552](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/71d0552fe48a56d31272258a97f5f4f714c95441))

# [9.0.0-preview.4](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.3...v9.0.0-preview.4) (2022-09-20)


### Bug Fixes

* Resurrects CassandraEventStorePlayer_v8 ([73b6e31](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/73b6e31739a0a6f68e19eb3def2ebacf5a45f982))

# [9.0.0-preview.3](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.2...v9.0.0-preview.3) (2022-09-19)


### Bug Fixes

* Remove all 'new' occurrences ([193deeb](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/193deebdd8a4c2ed03825a8f8f36b046e94e82d1))

# [9.0.0-preview.2](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v9.0.0-preview.1...v9.0.0-preview.2) (2022-09-19)


### Bug Fixes

* Code refactoring ([6e45892](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/6e45892f804c9ca5a9d06158a3b059dd8453d88a))

# [9.0.0-preview.1](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v8.0.5...v9.0.0-preview.1) (2022-09-14)


### Bug Fixes

* Fix solution after a huge merge ([859e829](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/859e82994c2423e044122132c58464ade0315185))

## [8.0.5](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v8.0.4...v8.0.5) (2022-08-15)


### Bug Fixes

* pipeline fix attempt 1 ([977c120](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/977c1206b7abc2fe5144cb0ad6c81e1090d5631e))

## [8.0.4](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v8.0.3...v8.0.4) (2022-08-10)


### Bug Fixes

* Try to deploy again [#2](https://github.com/Elders/Cronus.Persistence.Cassandra/issues/2) ([3477883](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/3477883161f3ff6b12da32f9790b458552d84038))

## [8.0.3](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v8.0.2...v8.0.3) (2022-08-10)


### Bug Fixes

* Try to deploy again ([75faedb](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/75faedb136f64fe758cee96732b3388f904b7d2f))

## [8.0.2](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v8.0.1...v8.0.2) (2022-08-10)


### Bug Fixes

* Update Cronus ([2bd64a9](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/2bd64a9a2a1dbc574d1dda2df9640940447a1edd))

## [8.0.1](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v8.0.0...v8.0.1) (2022-07-28)


### Bug Fixes

* Fix message counter reset ([c5e8ddc](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/c5e8ddcb42bfa08ba57f0109b357238b3aace390))

# [8.0.0](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v7.0.2...v8.0.0) (2022-05-25)


### Bug Fixes

* Ensure to prepare and execute queries asynchronously ([14577ab](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/14577ab2341471333ddaf4f767be2bab170ab0ed))
* Establish async connection to Cassandra session ([0c06db2](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/0c06db2b54645a7f43232e7212db5a4113a2e852))
* Fix merge effects ([105c05d](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/105c05da61b5e11a0b6830bd70f7ce32c4f4035c))
* Merge: fix inkection a non-generic logger ([36e60bf](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/36e60bfd3d4540813d756313183cb557c8e83e0e))
* Update Cronus package ([789eb1d](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/789eb1d1032c9aed4ae8474d4bfe8d2417351137))

# [8.0.0-preview.4](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v8.0.0-preview.3...v8.0.0-preview.4) (2022-05-19)


### Bug Fixes

* Add loggs when loading events ([f7ced3a](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/f7ced3af698d73bdb8a7984fd094a9784900d41d))
* Fix injecting a non-generic logger ([90326e4](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/90326e428950ae2273e1b64416762b4bd899d19c))
* Fix merge effects ([105c05d](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/105c05da61b5e11a0b6830bd70f7ce32c4f4035c))
* Merge: fix inkection a non-generic logger ([36e60bf](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/36e60bfd3d4540813d756313183cb557c8e83e0e))

# [8.0.0-preview.3](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v8.0.0-preview.2...v8.0.0-preview.3) (2022-04-18)


### Bug Fixes

* Update Cronus package ([789eb1d](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/789eb1d1032c9aed4ae8474d4bfe8d2417351137))

# [8.0.0-preview.2](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v8.0.0-preview.1...v8.0.0-preview.2) (2022-04-13)


### Bug Fixes

* Establish async connection to Cassandra session ([0c06db2](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/0c06db2b54645a7f43232e7212db5a4113a2e852))

# [8.0.0-preview.1](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v7.0.0...v8.0.0-preview.1) (2022-04-11)


### Bug Fixes

* Ensure to prepare and execute queries asynchronously ([14577ab](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/14577ab2341471333ddaf4f767be2bab170ab0ed))

# [7.0.0-preview.11](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v7.0.0-preview.10...v7.0.0-preview.11) (2022-04-11)

# [7.0.0-preview.10](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v7.0.0-preview.9...v7.0.0-preview.10) (2022-04-11)

# [7.0.0-preview.9](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v7.0.0-preview.8...v7.0.0-preview.9) (2022-04-05)

# [7.0.0-preview.8](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v7.0.0-preview.7...v7.0.0-preview.8) (2022-03-31)


### Bug Fixes

* Update Cronus package ([e8c70ea](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/e8c70eaaf179ead37a2e3c2082020a87df31724b))

# [7.0.0-preview.7](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v7.0.0-preview.6...v7.0.0-preview.7) (2022-03-22)


### Bug Fixes

* Removes session reload ([1213227](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/1213227fbf83dd98eff521bc60935d33ce8ea268))
* Updates packages ([16c2bf5](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/16c2bf59cc583f60d21c854476b25175b43dce7c))

# [7.0.0-preview.6](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v7.0.0-preview.5...v7.0.0-preview.6) (2022-03-07)


### Bug Fixes

* Add async version of Load ([4714ce4](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/4714ce48ca8c938717085f74b9de5d383ba21f79))

# [7.0.0-preview.5](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v7.0.0-preview.4...v7.0.0-preview.5) (2022-02-09)


### Bug Fixes

* We hope that we've fixed 'keyspace doesn't exist' in concurrent execution ([47bc65a](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/47bc65a9ec148d813d002f109d459d480725d99a))

# [7.0.0-preview.4](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v7.0.0-preview.3...v7.0.0-preview.4) (2021-12-20)


### Bug Fixes

* Makes ES index writing to C* async ([704bc13](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/704bc135ff9094d5e9c600a7e629fc6ac9a13d44))
* Update Cronus ([9cd8480](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/9cd8480848d3f4b49e72a81c8afb85654a0aed2b))

# [7.0.0-preview.3](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v7.0.0-preview.2...v7.0.0-preview.3) (2021-12-01)


### Bug Fixes

* Add missing parts for Event Store migrations ([f0f3e66](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/f0f3e661cc5f79c34d3d26c9508998276c1dc2c0))

# [7.0.0-preview.2](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v7.0.0-preview.1...v7.0.0-preview.2) (2021-11-30)


### Bug Fixes

* Update Cronus ([97db8c1](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/97db8c1269cf533d3481fd320717019f5f26438e))

# [7.0.0-preview.1](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v6.3.0...v7.0.0-preview.1) (2021-11-11)

# [6.3.0](https://github.com/Elders/Cronus.Persistence.Cassandra/compare/v6.2.4...v6.3.0) (2021-11-08)


### Bug Fixes

* Consolidates release notes ([4114a80](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/4114a803a2a0f037cef0a9f3374e480a4b403fb6))
* Fixes AggregateCommits loading ([3de7b23](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/3de7b2318c80ec7c01c68a3abb0956e84a44ba81))
* Fixes copyright attribute ([779131f](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/779131f70ca04d2384ee6bc46fd7fd1da684ab92))
* Fixes release flow ([94abd29](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/94abd296d122557935ef5b5a78963aa76dbc9e5d))
* Implements new replay option ([03b89b6](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/03b89b632ca92041787843acafe82a279a738c41))
* Prepare release ([99e5821](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/99e5821718e4975452e53cc617077e6b8f773428))
* Project Cleanup ([55b62f6](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/55b62f67fb3e7538b4748570b380d73a372b4a40))
* Removes gitversion ([42355cf](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/42355cfaf05e45c338dcd214bd0b6f25a29bbee6))
* Updates Cronus ([c7245f2](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/c7245f2b3d4c3188eafa5d92a1b452b1407d2778))
* Updates Cronus ([d64d259](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/d64d2591bde4be4026237c4a5e89430d3b036173))
* Updates packages ([d6bec5f](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/d6bec5f0b47351d6ee5d4bc81a46e0a2279ee460))


### Features

* Release ([28c6fe9](https://github.com/Elders/Cronus.Persistence.Cassandra/commit/28c6fe9f50b05e22fa89ae01929058a268398095))

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
