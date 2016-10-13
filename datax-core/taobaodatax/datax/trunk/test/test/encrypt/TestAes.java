package test.encrypt;

import java.security.AlgorithmParameterGenerator;
import java.security.AlgorithmParameters;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.sun.crypto.provider.AESCipher;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import com.taobao.datax.common.util.DecryptUtil;

import org.junit.Test;

/**
 * Created by zhuhq on 2016/4/1.
 */

public class TestAes {
    @Test
    public void testAesCbcMode() throws Exception{

        String charset = "utf-8";
        String keyStr = "key for datax500";
        String ps = "6qu5ZQza6YDEeLmkD75E75797BB625F5AD4EC692B4C29160B0F3E81F04F543E04EDD23D79C1D51E2";
        String iv = ps.substring(0,16);
        byte[] toDecrypt = new byte[32];
        int j = 0;
        for(int i = 16; i < ps.length(); i+=2) {
            toDecrypt[j] =(byte)Integer.parseInt(ps.substring(i, i + 2), 16);
            j += 1;

        }
        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        SecretKeySpec key = new SecretKeySpec(keyStr.getBytes(charset),"AES");
        cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv.getBytes(charset)));
        byte[] result  = cipher.doFinal(toDecrypt);
        System.out.print(new String(result,charset).trim());

    }

    @Test
    public void testDecryptUtil() {
        String decode = DecryptUtil.decrypt("6qu5ZQza6YDEeLmkD75E75797BB625F5AD4EC692B4C29160B0F3E81F04F543E04EDD23D79C1D51E2");
        System.out.println("decode:" + decode);
    }
}
