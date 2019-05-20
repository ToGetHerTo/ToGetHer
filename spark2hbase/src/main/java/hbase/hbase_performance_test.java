package hbase;

import kafka.MockerData;

/**
 * @author Lin
 * @date 2019/5/16 - 9:38
 */
public class hbase_performance_test{

    int a = 10010;

    static class thread1 extends Thread{
        @Override
        public void run() {
            super.run();
            for(int i=10001; i<=10030; i++){
                try {
                    String[] data = MockerData.mock().split(" ");
                    hbase_api.putDataForRowkey("default:stu",String.valueOf(i),"info1",data[0],data[1]);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            }
    }
    static class thread2 extends Thread{
        @Override
        public void run() {
            super.run();
            for(int i=10001; i<10010; i++){
                try {
                    String[] data = MockerData.mock1().split(" ");
                    hbase_api.putDataForRowkey("default:stu",String.valueOf(i),"info1",data[0],data[1]);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }static class thread3 extends Thread{
        @Override
        public void run() {
            super.run();
            for(int i=10001; i<10010; i++){
                try {
                    String[] data = MockerData.mock2().split(" ");
                    hbase_api.putDataForRowkey("default:stu",String.valueOf(i),"info1",data[0],data[1]);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

              new thread1().start();
           // new thread2().start();
            //new thread3().start();

        long endTime = System.currentTimeMillis();

        System.out.println("Times="+ (endTime-startTime)+"ms");
    }

}

