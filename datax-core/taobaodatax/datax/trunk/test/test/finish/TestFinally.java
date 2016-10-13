package test.finish;

import org.junit.Test;

/**
 * Created by zhuhq on 2016/3/31.
 */
public class TestFinally {
    static class  Result{
        String msg = "sucess";
        int code = 0;
    }

    public Result getResult() {
        Result result = new Result();
        try {
            result.msg = "changeTry";
            if(true) {throw new RuntimeException("");}
            return  result;

        }catch (Exception ex) {
            result.msg = "changeError";
        }finally {
            result.msg = "changeFinally";
        }
        return  result;
    }
    @Test
    public void testFinally() {
        System.out.println(getResult().msg);//changeFinally
        System.out.println(getResultCode());//3
    }
    public int getResultCode() {
        int result = 0;
        try {
            result = 1;
            if (true) {throw new RuntimeException("");}
            return  result;
        }catch (Exception ex) {
            result = 2;
        }finally {
            result = 3;
        }
        return  result;
    }
}

