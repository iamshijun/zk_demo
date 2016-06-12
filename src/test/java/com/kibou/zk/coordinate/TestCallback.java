package com.kibou.zk.coordinate;

interface TestCallback {
	public void beforeStart();
	public void beforeCompletion();
	public void afterCompletion();

	public TestCallback DEFAULT = new TestCallback() {
		public void beforeStart() {}
		public void beforeCompletion(){}
		public void afterCompletion() {}
	};
}

class TestCallbackAdpater implements TestCallback{
	public void beforeStart() {}
	public void beforeCompletion(){}
	public void afterCompletion() {}
}