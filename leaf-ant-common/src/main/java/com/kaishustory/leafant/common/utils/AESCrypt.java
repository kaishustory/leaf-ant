
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.kaishustory.leafant.common.utils;


import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Base64;


/**
 * ProjectName: commons <br/>
 * ClassName: AESCrypt <br/>
 * Date: 2017年5月3日 下午2:48:42 <br/>
 *
 * @author chababa
 * @see
 * @since JDK 1.7
 */
public class AESCrypt {
    public static final String SEED_16_CHARACTER = "ODZDNjUzNkFGMzVFRUQ0ODIxNEJDQ0YzM0MzMzE0MEQ=";
    private final Cipher cipher;
    private final SecretKeySpec key;
    private AlgorithmParameterSpec spec;

    public AESCrypt() throws Exception {
        this(SEED_16_CHARACTER);
    }

    public AESCrypt(String password) throws Exception {
        // hash password with SHA-256 and crop the output to 128-bit for key
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(password.getBytes("UTF-8"));
        byte[] keyBytes = new byte[32];
        System.arraycopy(digest.digest(), 0, keyBytes, 0, keyBytes.length);
        cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        key = new SecretKeySpec(keyBytes, "AES");
        spec = getIV();
    }

    public AlgorithmParameterSpec getIV() {
        byte[] iv = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,};
        IvParameterSpec ivParameterSpec;
        ivParameterSpec = new IvParameterSpec(iv);
        return ivParameterSpec;
    }

    public String encrypt(String plainText) throws Exception {
        cipher.init(Cipher.ENCRYPT_MODE, key, spec);
        byte[] encrypted = cipher.doFinal(plainText.getBytes("UTF-8"));
        String encryptedText = new String(Base64.getEncoder().encode(encrypted), "UTF-8");
        return encryptedText;
    }

    public String decrypt(String cryptedText) throws Exception {
        cipher.init(Cipher.DECRYPT_MODE, key, spec);
        byte[] bytes = Base64.getDecoder().decode(cryptedText);
        byte[] decrypted = cipher.doFinal(bytes);
        String decryptedText = new String(decrypted, "UTF-8");
        return decryptedText;
    }

}
