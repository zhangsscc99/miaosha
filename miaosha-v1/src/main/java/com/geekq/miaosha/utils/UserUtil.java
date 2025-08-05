package com.geekq.miaosha.utils;

import com.geekq.miaosha.domain.MiaoshaUser;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class UserUtil {

    private static void createUser(int count) throws Exception {
        List<MiaoshaUser> users = new ArrayList<MiaoshaUser>(count);
        //生成用户
        for (int i = 0; i < count; i++) {
            MiaoshaUser user = new MiaoshaUser();
            user.setId(100L + i);
            user.setLoginCount(1);
            user.setNickname(String.valueOf(13000000000L + i));
            user.setRegisterDate(new Date());
            user.setSalt("1a2b3c");
            user.setPassword(MD5Util.inputPassToDbPass("123456", user.getSalt()));
            users.add(user);
        }
        System.out.println("create user");
        //插入数据库
        Connection conn = DBUtil.getConn();
        String sql = "insert into miaosha_user(login_count, nickname, register_date, salt, password, id)values(?,?,?,?,?,?)";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (int i = 0; i < users.size(); i++) {
            MiaoshaUser user = users.get(i);
            pstmt.setInt(1, user.getLoginCount());
            pstmt.setString(2, user.getNickname());
            pstmt.setTimestamp(3, new Timestamp(user.getRegisterDate().getTime()));
            pstmt.setString(4, user.getSalt());
            pstmt.setString(5, user.getPassword());
            pstmt.setLong(6, user.getId());
            pstmt.addBatch();
        }
        pstmt.executeBatch();
        pstmt.close();
        conn.close();
        System.out.println("insert to db");
        //登录，生成token
        String urlString = "http://localhost:8080/login/create_token";
        File file = new File("C:/Users/ZhuanZ（无密码）/Desktop/miaosha/tokens.txt");
        if (file.exists()) {
            file.delete();
        }
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        file.createNewFile();
        raf.seek(0);
        for (int i = 0; i < users.size(); i++) {
            MiaoshaUser user = users.get(i);
            URL url = new URL(urlString);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setRequestMethod("POST");
            co.setDoOutput(true);
            OutputStream out = co.getOutputStream();
            String params = "mobile=" + user.getNickname() + "&password=" + MD5Util.inputPassToFormPass("123456");
            out.write(params.getBytes());
            out.flush();
            InputStream inputStream = co.getInputStream();
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            byte buff[] = new byte[1024];
            int len = 0;
            while ((len = inputStream.read(buff)) >= 0) {
                bout.write(buff, 0, len);
            }
            inputStream.close();
            bout.close();
            String response = new String(bout.toByteArray());
//			JSONObject jo = JSON.parseObject(response);
//			String token = jo.getString("data");
            System.out.println("create token : " + user.getId());

            String row = user.getNickname() + "," + response;
            raf.seek(raf.length());
            raf.write(row.getBytes());
            raf.write("\r\n".getBytes());
            System.out.println("write to file : " + user.getId());
        }
        raf.close();

        System.out.println("over");
    }

    public static void main(String[] args) throws Exception {
        // createUser(1000);  // 注释掉创建用户，因为数据库中已有用户
        loginAndGenerateTokens(1000);  // 直接生成tokens
    }
    
    /**
     * 为现有用户生成tokens
     */
    public static void loginAndGenerateTokens(int count) throws Exception {
        File file = new File("C:/Users/ZhuanZ（无密码）/Desktop/miaosha/tokens.txt");
        if (file.exists()) {
            file.delete();
        }
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        file.createNewFile();
        raf.seek(0);
        
        for (int i = 0; i < count; i++) {
            // 使用数据库中实际存在的用户ID和nickname格式
            Long userId = 13000000000L + i;
            String userNickname = "user" + i;
            System.out.println("create token for user:" + userId);

            // 第一步：先登录获取session
            String loginUrl = "http://localhost:8080/login/do_login";
            URL url = new URL(loginUrl);
            HttpURLConnection loginConn = (HttpURLConnection) url.openConnection();
            loginConn.setRequestMethod("POST");
            loginConn.setDoOutput(true);
            
            OutputStream out = loginConn.getOutputStream();
            String loginParams = "mobile=" + userNickname + "&password=" + MD5Util.inputPassToFormPass("123456");
            out.write(loginParams.getBytes());
            out.flush();
            
            // 获取登录后的cookies
            String cookies = "";
            String headerName = null;
            for (int j = 1; (headerName = loginConn.getHeaderFieldKey(j)) != null; j++) {
                if (headerName.equals("Set-Cookie")) {
                    String cookie = loginConn.getHeaderField(j);
                    cookies += cookie + "; ";
                }
            }
            
            // 读取登录响应（虽然我们主要需要cookie）
            InputStream loginInputStream = loginConn.getInputStream();
            ByteArrayOutputStream loginBout = new ByteArrayOutputStream();
            byte loginBuff[] = new byte[1024];
            int loginLen = 0;
            while ((loginLen = loginInputStream.read(loginBuff)) >= 0) {
                loginBout.write(loginBuff, 0, loginLen);
            }
            loginInputStream.close();
            loginBout.close();
            loginConn.disconnect();
            
            System.out.println("Login successful, cookies: " + cookies);

            // 第二步：使用获取的session调用create_token接口
            String tokenUrl = "http://localhost:8080/login/create_token";
            URL tokenUrlObj = new URL(tokenUrl);
            HttpURLConnection tokenConn = (HttpURLConnection) tokenUrlObj.openConnection();
            tokenConn.setRequestMethod("POST");
            tokenConn.setDoOutput(true);
            
            // 重要：设置从登录获取的cookies
            if (cookies.length() > 0) {
                tokenConn.setRequestProperty("Cookie", cookies);
            }
            
            OutputStream tokenOut = tokenConn.getOutputStream();
            String tokenParams = "mobile=" + userNickname + "&password=" + MD5Util.inputPassToFormPass("123456");
            tokenOut.write(tokenParams.getBytes());
            tokenOut.flush();
            
            InputStream tokenInputStream = tokenConn.getInputStream();
            ByteArrayOutputStream tokenBout = new ByteArrayOutputStream();
            byte tokenBuff[] = new byte[1024];
            int tokenLen = 0;
            while ((tokenLen = tokenInputStream.read(tokenBuff)) >= 0) {
                tokenBout.write(tokenBuff, 0, tokenLen);
            }
            tokenInputStream.close();
            tokenBout.close();
            String response = new String(tokenBout.toByteArray());
            tokenConn.disconnect();
            
            // 由于create_token端点返回的是普通字符串token，不是JSON
            String token = response.trim(); // 去除可能的空白字符
            System.out.println("token:" + token);

            String row = userId + "," + token;
            raf.seek(raf.length());
            raf.write(row.getBytes());
            raf.write("\r\n".getBytes());
        }
        raf.close();
        System.out.println("tokens generated successfully!");
    }
}
