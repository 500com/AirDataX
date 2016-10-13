package test.parquet;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.taobao.datax.common.exception.ExceptionTracker;

import org.junit.Test;

/**
 * Created by zhuhq on 2016/8/3.
 */
public class ParquetTest {
    @Test
    public void testType() throws Exception{
        InputStream is = null;
        try {
            is = new FileInputStream(new File("d:\\tmp\\000000_2"));

            System.out.println((char)is.read());
            System.out.println((char)is.read());
        }catch (Exception ex) {

        }finally {
            if(is != null) {
                is.close();
            }
        }


    }
}
