#! /usr/bin/env tarantool
box.cfg{
	listen=3302,
	replication_source='replicator:pwd@localhost:3303'
}
