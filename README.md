`mysql_binlog` is a clean, idomatic Rust implementation of a MySQL binlog parser, including support for the JSONB type introduced in MySQL 5.7.

[![Build Status](https://travis-ci.com/EasyPost/rust-mysql-binlog.svg?branch=master)](https://travis-ci.com/EasyPost/rust-mysql-binlog)
[![crates.io](https://meritbadge.herokuapp.com/mysql_binlog)](https://crates.io/crates/mysql_binlog)
[![docs](https://docs.rs/mysql_binlog/badge.svg)](https://docs.rs/mysql_binlog)

Its primary purpose is handling row-based logging messages, but it has rudimentary support for older statement-based replication. It's been tested against Percona XtraDB (MySQL) 5.6 and 5.7.

This library seeks to be competitive with `mysqlbinlog` at time-to-parse a full binlog file, and is already several orders of magnitude faster than `go-mysql`, `python-mysql-replication`, or Ruby's `mysql_binlog`. All interesting datatypes are serializable using Serde, so it's easy to hook into other data processing flows.
