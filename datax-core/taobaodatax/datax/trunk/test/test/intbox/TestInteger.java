package test.intbox;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhuhq on 2015/11/13.
 */
public class TestInteger {
    @Test
    public void addOneTest() {
        Integer i = 0;
        i += 1;
        System.out.println(i);//1
        Map<String,Integer> ints = new HashMap<String,Integer>();
        ints.put("int",i);
        Integer y = ints.get("int");//y = 1
        y += 1;//y=2
        System.out.println(ints.get("int"));//print 1
        Integer n=2;
        n = n == null ?1:++n;
        System.out.println(n);

    }
}
