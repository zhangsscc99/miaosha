package com.geekq.miaosha.utils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Simple Token Generator for Load Testing
 * Can run independently without Spring Boot context
 */
public class TokenGenerator {
    
    public static void main(String[] args) throws Exception {
        System.out.println("Starting token generation...");
        
        // Check if application is running
        if (!isAppRunning()) {
            System.err.println("ERROR: Application is not running!");
            System.err.println("Please start the application first: mvn spring-boot:run");
            return;
        }
        
        generateTokens(50); // Generate 50 tokens for testing
    }
    
    /**
     * Check if application is running
     */
    private static boolean isAppRunning() {
        try {
            URL url = new URL("http://localhost:8080/goods/to_list");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(3000);
            int responseCode = conn.getResponseCode();
            conn.disconnect();
            return responseCode == 200;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Generate user tokens
     */
    public static void generateTokens(int count) throws Exception {
        File file = new File("tokens.txt");
        if (file.exists()) {
            file.delete();
        }
        
        FileWriter writer = new FileWriter(file);
        int successCount = 0;
        
        for (int i = 0; i < count; i++) {
            try {
                String username = "user" + i;
                String password = "123456";
                
                // First login to get session
                String loginResult = doLogin(username, password);
                if (loginResult != null && !loginResult.contains("error")) {
                    // Then get token
                    String token = getToken(username, password);
                    if (token != null && !token.trim().isEmpty()) {
                        writer.write((13000000000L + i) + "," + token.trim() + "\n");
                        successCount++;
                        System.out.println("Token generated successfully: " + username + " -> " + token.trim());
                    }
                }
                
                // Avoid too fast requests
                Thread.sleep(100);
                
            } catch (Exception e) {
                System.err.println("Failed to generate token for user" + i + " - " + e.getMessage());
            }
        }
        
        writer.close();
        System.out.println("Token generation completed! Successfully generated " + successCount + "/" + count + " tokens");
        System.out.println("Token file saved as: tokens.txt");
    }
    
    /**
     * Login to get session
     */
    private static String doLogin(String username, String password) throws Exception {
        URL url = new URL("http://localhost:8080/login/do_login");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        
        String params = "mobile=" + username + "&password=" + password;
        
        try (OutputStream os = conn.getOutputStream()) {
            os.write(params.getBytes());
        }
        
        StringBuilder response = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
        }
        
        conn.disconnect();
        return response.toString();
    }
    
    /**
     * Get Token
     */
    private static String getToken(String username, String password) throws Exception {
        URL url = new URL("http://localhost:8080/login/create_token");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        
        String params = "mobile=" + username + "&password=" + password;
        
        try (OutputStream os = conn.getOutputStream()) {
            os.write(params.getBytes());
        }
        
        StringBuilder response = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
        } catch (IOException e) {
            // If reading fails, try to read error stream
            try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(conn.getErrorStream()))) {
                String line;
                while ((line = errorReader.readLine()) != null) {
                    response.append(line);
                }
            }
        }
        
        conn.disconnect();
        return response.toString();
    }
}