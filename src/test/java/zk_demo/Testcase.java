package zk_demo;

import org.junit.Test;

public class Testcase {

	static{
		System.out.println("static");
	}
	
	class X {
//	    Y b = new Y();
		final int f;
		int i = 1;
		int z;
		{
		   System.out.print("'X{}'");
		   //System.out.print(j);和静态块一样,对后续的属性变量只能写不能读
		   j = 3;
		}
	    X() {
	        System.out.print("X");
	        f = 10;
	        System.out.print(i);
	        System.out.print(j);
	    }
	    int j = 2;
	    Y b = new Y();//print "Y";
	}
	/*
	 *  javap -v 反编译可以看到
	 *  code: X.init:()V
	 *    Object.<init>:()V
	 *    i = 1 (i定义在最前面)
	 *    print 'X{}'  ({}块级语句在i下) 
	 *    j = 3
	 *    j = 2
	 *    'temp' = allocate mem to Y;
	 *    Y.<init>:()V
	 *    b = 'temp';
	 *    /////这一这里才开始到我们代码中的X() 构造函数块中
	 *    print x;
	 *    get i & print i; 
	 *    get j & print j;
	 *    //ps z的零值在构造函数之前被赋予了,即后续如果没有像i这样的赋值,就不会出现在构造函数体里了
	 *    可见对于非(static)属性赋值其实被编译器放在了 代码构造函数之前完成的,上述的反编译结果可以看出顺序来
	 */
	

	class Y {
	    Y() {
	        System.out.print("Y");
	    }
	}

	class Z extends X {
	    Y y = new Y();

	    Z() {
	        System.out.print("Z");
	    }
	}

	@Test
	public void test01(){
		new Z();
	}
	
	public static void main(String[] args) {
		//new Testcase();
	}
}
