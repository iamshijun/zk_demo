# WatcherEvent
	path(znode path), state(:KeeperState), type(:EventType)
# EventType
	all abount znode(None,NodeCreated,NodeDeleted,NodeDataChanged,NodeChildrenChanged)
# Code
	aka Reason code ,can be reach in AsynCallback
# Stat
	znode stat(version,time,....)
# KeeperState
	zookeeper server state