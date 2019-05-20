package kafka;

import java.util.Date;
import java.util.Random;

/**
 * @author Lin
 * @date 2019/5/13 - 10:09
 */
public class MockerData {

    public static int keyRandom(){
        Random r = new Random();
        int a = r.nextInt(100);
        return a;
    }
    public static int keyRandom2(){
        Random r = new Random();
        int a = r.nextInt(999)%(999-100+1) + 100;
        return a;
    }
    public static int keyRandom3(){
        Random r = new Random();
        int a = r.nextInt(9999)%(9999-1000+1) + 1000;
        return a;
    }
    public static String valueBinary() {
        int num = (int) new Date().getTime();
        String num1 = Integer.toBinaryString(num);
        return num1;
    }

    public static String mock() throws Exception{
        String data = keyRandom() + " " + valueBinary();
        //System.out.println(data);
        Thread.sleep(500);
        return data;
    }
    public static String mock1() throws Exception{
        String data = keyRandom2() + " " + valueBinary();
        //System.out.println(data);
        Thread.sleep(500);
        return data;
    }
    public static String mock2() throws Exception{
        String data = keyRandom3() + " " + valueBinary();
        //System.out.println(data);
        Thread.sleep(500);
        return data;
    }

    public static void main(String[] args) throws Exception{
       // mock();
        System.out.println(keyRandom2());
    }

}


