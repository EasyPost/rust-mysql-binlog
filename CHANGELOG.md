# ChangeLog

## [0.4.0] - 2022-08-22

 - Improve how `CHAR`, `BINARY`, and `MEDIUMBLOB` columns are handled (thanks @yonran in #2)
 - Upgrade `uuid` dependency to version 1

## [0.3.2] - 2022-01-18

 - Improve how `serde` is imported
 - Bump `bigdecimal` to 0.3

## [0.3.1] - 2020-08-31

 - handle 3-byte varint length prefixes on blobs
 - update from `bigdecimal` 0.0 to `bigdecimal` 0.1

## [0.3.0] - 2020-05-05

 - expose logical timestamps when available (so you can debug parallel replication)
 - expose offsets in event iterator
 - move from [failure](https://github.com/rust-lang-nursery/failure) to normal Error structs (aided by [thiserror](https://github.com/dtolnay/thiserror))
 - bump dependencies

## [0.2.0] - 2019-02-15

 - clean up a bunch of APIs to work better as a library
 - add more tests
 - add more documentation

## [0.1.0] - 2019-02-15

 - initial release
