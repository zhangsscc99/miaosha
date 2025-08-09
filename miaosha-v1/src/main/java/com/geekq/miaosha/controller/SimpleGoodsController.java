package com.geekq.miaosha.controller;

import com.geekq.miaosha.domain.MiaoshaUser;
import com.geekq.miaosha.service.GoodsService;
import com.geekq.miaosha.vo.GoodsVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

/**
 * Simple Goods Controller for Load Testing
 * Bypasses Redis cache for testing purposes
 */
@Controller
@RequestMapping("/simple")
public class SimpleGoodsController {

    @Autowired
    GoodsService goodsService;

    /**
     * Simple goods list without Redis cache
     */
    @RequestMapping("/goods/list")
    @ResponseBody
    public String goodsList(HttpServletRequest request, HttpServletResponse response, Model model, MiaoshaUser user) {
        try {
            List<GoodsVo> goodsList = goodsService.listGoodsVo();
            
            StringBuilder result = new StringBuilder();
            result.append("{\n  \"code\": 0,\n  \"msg\": \"success\",\n  \"data\": [\n");
            
            for (int i = 0; i < goodsList.size(); i++) {
                GoodsVo goods = goodsList.get(i);
                if (i > 0) result.append(",\n");
                result.append("    {\n");
                result.append("      \"goodsId\": ").append(goods.getId()).append(",\n");
                result.append("      \"goodsName\": \"").append(goods.getGoodsName()).append("\",\n");
                result.append("      \"goodsTitle\": \"").append(goods.getGoodsTitle()).append("\",\n");
                result.append("      \"goodsPrice\": ").append(goods.getGoodsPrice()).append(",\n");
                result.append("      \"miaoshaPrice\": ").append(goods.getMiaoshaPrice()).append(",\n");
                result.append("      \"stockCount\": ").append(goods.getStockCount()).append("\n");
                result.append("    }");
            }
            
            result.append("\n  ]\n}");
            return result.toString();
            
        } catch (Exception e) {
            return "{\"code\": 500, \"msg\": \"" + e.getMessage() + "\"}";
        }
    }

    /**
     * Simple goods detail without Redis cache
     */
    @RequestMapping("/goods/detail/{goodsId}")
    @ResponseBody
    public String goodsDetail(HttpServletRequest request, HttpServletResponse response, Model model, 
                            MiaoshaUser user, @PathVariable("goodsId") long goodsId) {
        try {
            GoodsVo goods = goodsService.getGoodsVoByGoodsId(goodsId);
            if (goods == null) {
                return "{\"code\": 404, \"msg\": \"Goods not found\"}";
            }
            
            StringBuilder result = new StringBuilder();
            result.append("{\n  \"code\": 0,\n  \"msg\": \"success\",\n  \"data\": {\n");
            result.append("    \"goodsId\": ").append(goods.getId()).append(",\n");
            result.append("    \"goodsName\": \"").append(goods.getGoodsName()).append("\",\n");
            result.append("    \"goodsTitle\": \"").append(goods.getGoodsTitle()).append("\",\n");
            result.append("    \"goodsPrice\": ").append(goods.getGoodsPrice()).append(",\n");
            result.append("    \"miaoshaPrice\": ").append(goods.getMiaoshaPrice()).append(",\n");
            result.append("    \"stockCount\": ").append(goods.getStockCount()).append(",\n");
            result.append("    \"startDate\": \"").append(goods.getStartDate()).append("\",\n");
            result.append("    \"endDate\": \"").append(goods.getEndDate()).append("\"\n");
            result.append("  }\n}");
            
            return result.toString();
            
        } catch (Exception e) {
            return "{\"code\": 500, \"msg\": \"" + e.getMessage() + "\"}";
        }
    }

    /**
     * Health check endpoint
     */
    @RequestMapping("/health")
    @ResponseBody
    public String health() {
        return "{\"status\": \"ok\", \"timestamp\": " + System.currentTimeMillis() + "}";
    }
}