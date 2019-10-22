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

import lombok.SneakyThrows;
import lombok.val;
import okhttp3.*;

import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;

/**
 * Http工具类
 *
 * @author liguoyang
 * @create 2019-09-25 15:46
 **/
public class HttpUtils {

    /**
     * 请求
     */
    private static OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .writeTimeout(1, TimeUnit.MINUTES)
            .readTimeout(2, TimeUnit.MINUTES)
            .build();

    /**
     * GET 请求
     * @param url 请求地址
     * @return 返回内容
     */
    @SneakyThrows
    public static String get(String url) {
        val request = new Request.Builder().url(url).addHeader("secret", SecretUtils.getSecret()).addHeader("userid", "88888888").get().build();
        val response = client.newCall(request).execute();
        val body = response.body().string();
        if (response.isSuccessful()) {
            Log.info("GET请求成功 \nurl：{} \nstatus：{}", url, response.code());
            Log.debug("GET请求 body：{}", body);
        } else {
            Log.error("GET请求失败 \nurl：{} \nstatus：{} \nbody：{}", url, response.code(), body);
        }
        return body;
    }

    /**
     * GET 请求文件
     * @param url 请求地址
     * @return 返回文件
     */
    public static byte[] getFile(String url) {
        return getImage(url);
    }

    /**
     * GET 请求图片
     * @param url 请求地址
     * @return 返回图片
     */
    @SneakyThrows
    public static byte[] getImage(String url) {
        val request = new Request.Builder().url(url).header("secret", SecretUtils.getSecret()).get().build();
        val response = client.newCall(request).execute();
        if (response.isSuccessful()) {
            Log.info("GET文件请求成功 \nurl：{} \nstatus：{}", url, response.code());
        } else {
            Log.errorThrow("GET文件请求失败 \nurl：{} \nstatus：{}", url, response.code());
        }
        return response.body().bytes();
    }

    /**
     * POST 图片上传
     * @param url 请求地址
     * @param image 图片字节码
     */
    @SneakyThrows
    public static String postUploadImage(String url, byte[] image) {
        val requestBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart(
                        "img",
                        "showqrcode.jpeg",
                        RequestBody.create(MediaType.parse("image/jpeg"), image, 0, image.length))
                .build();
        val request = new Request.Builder().url(url).header("secret", SecretUtils.getSecret()).post(requestBody).build();
        val response = client.newCall(request).execute();
        val body = response.body().string();
        if (response.isSuccessful()) {
            Log.info("POST文件上传成功 \nurl：{} \nstatus：{} \nbody：{}", url, response.code(), body);
        } else {
            Log.errorThrow("POST文件上传失败 \nurl：{} \nstatus：{} \nbody：{}", url, response.code(), body);
        }
        return body;
    }

    @SneakyThrows
    private static String postUploadImageWithFileInfo(String url, byte[] image, String filename, String fileType) {
        val requestBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart(
                        "img",
                        filename,
                        RequestBody.create(MediaType.parse(fileType), image, 0, image.length))
                .build();

        val request = new Request.Builder().url(url).header("secret", SecretUtils.getSecret()).post(requestBody).build();
        val response = client.newCall(request).execute();
        val body = response.body().string();
        if (response.isSuccessful()) {
            Log.info("POST文件上传成功 \nurl：{} \nstatus：{} \nbody：{}", url, response.code(), body);
        } else {
            Log.errorThrow("POST文件上传失败 \nurl：{} \nstatus：{} \nbody：{}", url, response.code(), body);
        }
        return body;
    }

    /**
     * POST JSON请求
     * @param url 请求地址
     * @param json 请求体
     * @return 返回内容
     */
    @SneakyThrows
    public static String postJson(String url, String json) {
        val requestBody = FormBody.create(MediaType.parse("application/json; charset=utf-8"), json);
        val request = new Request.Builder().url(url).header("secret", SecretUtils.getSecret()).post(requestBody).build();
        val response = client.newCall(request).execute();
        val body = response.body().string();
        if (response.isSuccessful()) {
            Log.info("POST请求成功 \nurl：{} \nstatus：{}", url, response.code());
            Log.debug("POST请求 body：{}", body);
        } else {
            Log.errorThrow("POST请求失败 \nurl：{} \nstatus：{} \nbody：{}", url, response.code(), body);
        }
        return body;
    }

    /**
     * DELETE 请求
     * @param url 请求地址
     * @return 返回内容
     */
    public static String delete(String url) {
        try {
            val request = new Request.Builder().url(url).header("secret", SecretUtils.getSecret()).delete().build();
            val response = client.newCall(request).execute();
            val body = response.body().string();
            if (response.isSuccessful()) {
                Log.info("DELETE请求成功 \nurl：{} \nstatus：{} \nbody：{}", url, response.code(), body);
            } else {
                Log.errorThrow("DELETE请求失败 \nurl：{} \nstatus：{} \nbody：{}", url, response.code(), body);
            }
            return body;
        } catch (Exception e) {
            Log.errorThrow("DELETE请求发生异常 url：$url", e);
            return null;
        }
    }

    /**
     * 异步Get请求
     * @param url String
     */
    public static void asyncGet(String url, Callback callback) {
        asyncGet(url, null, null, callback);
    }

    /**
     * 异步Get请求
     * @param url String
     * @param timeout 请求超时时间
     * @param timeUnit 超时时间单位
     * @param callback 请求回调处理
     */
    public static void asyncGet(String url, Long timeout, TimeUnit timeUnit, Callback callback) {

        // 重设Http请求超时时间
        val newClient = timeout != null ?
            client.newBuilder().readTimeout(timeout, timeUnit).build() :
            client;
        // 请求
        val request = new Request.Builder().url(url).header("secret", SecretUtils.getSecret()).get().build();
        // 异步执行请求
        newClient.newCall(request).enqueue(callback);
    }

    /**
     * POST JSON请求
     * @param url 请求地址
     * @param json 请求体
     * @param callback 请求回调处理
     */
    public static void asyncPostJson(String url, String json, Callback callback) {
        asyncPostJson(url, json, null, null, callback);
    }

    /**
     * POST JSON请求
     * @param url 请求地址
     * @param json 请求体
     * @param timeout 请求超时时间
     * @param timeUnit 超时时间单位
     * @param callback 请求回调处理
     */
    public static void asyncPostJson(String url, String json, Long timeout, TimeUnit timeUnit, Callback callback) {
        // 重设Http请求超时时间
        val newClient = timeout != null ? client.newBuilder().readTimeout(timeout, timeUnit).build() : client;
        val requestBody = FormBody.create(MediaType.parse("application/json; charset=utf-8"), json);
        val request = new Request.Builder().url(url).header("secret", SecretUtils.getSecret()).post(requestBody).build();
        newClient.newCall(request).enqueue(callback);
    }

    /**
     * GET 请求 (单向请求)
     * @param url 请求地址
     * @return 返回内容
     */
    @SneakyThrows
    public static boolean getOne(String url) {
        try {
            val request = new Request.Builder().url(url).addHeader("secret", SecretUtils.getSecret()).addHeader("userid", "88888888").get().build();
            val response = client.newBuilder().readTimeout(10, TimeUnit.MILLISECONDS).build().newCall(request).execute();
            val body = response.body().string();
            if (response.isSuccessful() || response.code() == 408) {
                Log.info("GET请求成功 \nurl：{} \nstatus：{}", url, response.code());
                return true;
            } else {
                Log.error("GET请求失败 \nurl：{} \nstatus：{} \nbody：{}", url, response.code(), body);
                return false;
            }
        } catch (SocketTimeoutException e) {
            Log.info("GET请求成功 \nurl：{}", url);
            return true;
        } catch (Exception e) {
            Log.error("GET请求失败 \nurl：{}", url, e);
            return false;
        }
    }

    /**
     * POST JSON请求 (单向请求)
     * @param url 请求地址
     * @param json 请求体
     * @return 返回内容
     */
    @SneakyThrows
    public static boolean postJsonOne(String url, String json) {
        try {
            val requestBody = FormBody.create(MediaType.parse("application/json; charset=utf-8"), json);
            val request = new Request.Builder().url(url).header("secret", SecretUtils.getSecret()).post(requestBody).build();
            val response = client.newBuilder().readTimeout(10, TimeUnit.MILLISECONDS).build().newCall(request).execute();
            val body = response.body().string();
            if (response.isSuccessful() || response.code() == 408) {
                Log.info("POST请求成功 \nurl：{} \nstatus：{}", url, response.code());
                return true;
            } else {
                Log.error("POST请求失败 \nurl：{} \nstatus：{} \nbody：{}", url, response.code(), body);
                return false;
            }
        } catch (SocketTimeoutException e) {
            Log.info("GET请求成功 \nurl：{}", url);
            return true;
        } catch (Exception e) {
            Log.error("GET请求失败 \nurl：{}", url, e);
            return false;
        }
    }
}
