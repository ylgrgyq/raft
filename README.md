# raft

[![Build Status](https://travis-ci.com/ylgrgyq/raft.svg?branch=master)](https://travis-ci.com/ylgrgyq/raft)

Write a raft to know more about it.

Supported raft features:
- Leader election
- Log replication
- Membership changes
- Leadership transfer

## Testing coverage report

Element | Class, % | Method, % | Line, % 
------- | -------- | --------- | ----------
raft.server	| 91% (22/24)	| 88% (170/193)	| 83% (893/1064)
raft.server.log | 	60% (3/5) |	76% (29/38)	| 78% (142/182)
raft.server.storage	 | 96% (29/30) |	96% (187/193) |	91% (1066/1169)

