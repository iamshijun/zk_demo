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
		   //System.out.print(j);�;�̬��һ��,�Ժ��������Ա���ֻ��д���ܶ�
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
	 *  javap -v ��������Կ���
	 *  code: X.init:()V
	 *    Object.<init>:()V
	 *    i = 1 (i��������ǰ��)
	 *    print 'X{}'  ({}�鼶�����i��) 
	 *    j = 3
	 *    j = 2
	 *    'temp' = allocate mem to Y;
	 *    Y.<init>:()V
	 *    b = 'temp';
	 *    /////��һ����ſ�ʼ�����Ǵ����е�X() ���캯������
	 *    print x;
	 *    get i & print i; 
	 *    get j & print j;
	 *    //ps z����ֵ�ڹ��캯��֮ǰ��������,���������û����i�����ĸ�ֵ,�Ͳ�������ڹ��캯��������
	 *    �ɼ����ڷ�(static)���Ը�ֵ��ʵ�������������� ���빹�캯��֮ǰ��ɵ�,�����ķ����������Կ���˳����
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
