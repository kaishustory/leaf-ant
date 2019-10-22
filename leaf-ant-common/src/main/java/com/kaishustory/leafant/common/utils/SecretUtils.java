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


import java.util.Base64;
import java.util.Calendar;

/**
 * 秘钥工具
 *
 * @author liguoyang
 * @create 2019-02-12 6:13 PM
 **/
public class SecretUtils {

    /**
     * 秘钥因子
     */
    private static final String secretFactor = "Kaishu2099";

    /**
     * 生成一分钟秘钥 (默认秘钥)
     */
    public static String getSecret() {
        return getSecret(1, Calendar.MINUTE, secretFactor);
    }

    /**
     * 生成秘钥 (默认秘钥)
     *
     * @param time 过期时间
     * @param unit 单位（Calendar.MINUTE）
     * @return
     */
    public static String getSecret(int time, int unit) {
        return getSecret(time, unit, secretFactor);
    }

    /**
     * 生成秘钥
     *
     * @param time   过期时间
     * @param unit   单位（Calendar.MINUTE）
     * @param factor 秘钥因子
     * @return
     */
    public static String getSecret(int time, int unit, String factor) {
        try {
            //设置过期时间
            Calendar calendar = Calendar.getInstance();
            calendar.add(unit, time);

            //AES加密
            AESCrypt aesCrypt = new AESCrypt(factor);
            return new String(Base64.getEncoder().encode(aesCrypt.encrypt(calendar.getTimeInMillis() + "").getBytes()));
        } catch (Exception e) {
            Log.error("秘钥生成发生异常！", e);
            return null;
        }
    }

    /**
     * 验证秘钥是否有效 (默认秘钥)
     *
     * @param secret 秘钥
     * @return 是否有效
     */
    public static boolean validSecret(String secret) {
        return validSecret(secret, secretFactor);
    }


    /**
     * 验证秘钥是否有效 (默认秘钥) 验证通过处理
     *
     * @param secret      秘钥
     * @param vaildHandle 通过处理
     * @param <T>         返回类型
     * @return 处理结果
     */
    public static <T> Option<T> validSecret(String secret, VaildHandle<T> vaildHandle) {
        if (SecretUtils.validSecret(secret)) {
            return Option.of(vaildHandle.passHandle());
        } else {
            Log.error("秘钥错误! ");
            return Option.error("秘钥错误!");
        }
    }

    /**
     * 验证秘钥是否有效
     *
     * @param secret 秘钥
     * @param factor 秘钥因子
     * @return 是否有效
     */
    public static boolean validSecret(String secret, String factor) {
        try {
            //AES解密
            AESCrypt aesCrypt = new AESCrypt(factor);
            long timeout = Long.parseLong(aesCrypt.decrypt(new String(Base64.getDecoder().decode(secret))));
            //计算剩余有效期
            return timeout - System.currentTimeMillis() > 0;
        } catch (Exception e) {
            Log.warn("secret 秘钥错误！" + secret);
            return false;
        }
    }


    public interface VaildHandle<T> {
        /**
         * 验证通过处理
         */
        T passHandle();
    }
}
