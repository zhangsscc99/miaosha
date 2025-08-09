#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
秒杀系统压测数据生成脚本
基于数据库中现有的用户数据生成JMeter需要的CSV文件
"""

def generate_users_csv(user_count=1000):
    """
    生成用户CSV文件
    数据库中的用户格式：
    - ID: 13000000000 + i (从0开始)  
    - nickname: user + i
    - password: 123456 (明文，JMeter中会进行MD5加密)
    """
    with open('users.csv', 'w', encoding='utf-8') as f:
        f.write('userId,username,password\n')
        for i in range(user_count):
            user_id = 13000000000 + i
            username = f"user{i}"
            password = "123456"  # 原始密码，JMeter中处理加密
            f.write(f'{user_id},{username},{password}\n')
    print(f"生成了 {user_count} 个用户数据到 users.csv")

def generate_goods_csv():
    """
    根据数据库中的商品数据生成商品CSV
    """
    goods_data = [
        (1, "iphoneX", "Apple iPhone X (A1865) 64GB 银色", 8765.00, 10000),
        (2, "华为Meta9", "华为 Mate 9 4GB+32GB版 月光银", 3212.00, -1),
        (3, "iphone8", "Apple iPhone 8 (A1865) 64GB 银色", 5589.00, 10000),
        (4, "小米6", "小米6 4GB+32GB版 月光银", 3212.00, 10000)
    ]
    
    with open('goods.csv', 'w', encoding='utf-8') as f:
        f.write('goodsId,goodsName,goodsTitle,goodsPrice,goodsStock\n')
        for goods_id, name, title, price, stock in goods_data:
            f.write(f'{goods_id},{name},{title},{price},{stock}\n')
    print("生成了商品数据到 goods.csv")

def generate_miaosha_goods_csv():
    """
    生成秒杀商品CSV
    """
    miaosha_goods = [
        (1, 1, 0.01, 9),  # iphoneX秒杀
        (2, 2, 0.01, 9),  # 华为Meta9秒杀  
        (3, 3, 0.01, 9),  # iphone8秒杀
        (4, 4, 0.01, 9)   # 小米6秒杀
    ]
    
    with open('miaosha_goods.csv', 'w', encoding='utf-8') as f:
        f.write('miaoshaId,goodsId,miaoshaPrice,stockCount\n')
        for miaosha_id, goods_id, price, stock in miaosha_goods:
            f.write(f'{miaosha_id},{goods_id},{price},{stock}\n')
    print("生成了秒杀商品数据到 miaosha_goods.csv")

def generate_tokens_csv():
    """
    读取UserUtil生成的tokens.txt文件，转换为CSV格式
    如果tokens.txt不存在，提示先运行UserUtil
    """
    try:
        with open('tokens.txt', 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        with open('user_tokens.csv', 'w', encoding='utf-8') as f:
            f.write('userId,token\n')
            for line in lines:
                line = line.strip()
                if ',' in line:
                    user_id, token = line.split(',', 1)
                    f.write(f'{user_id},{token}\n')
        print("tokens.txt转换为user_tokens.csv成功")
    except FileNotFoundError:
        print("tokens.txt文件不存在，请先运行UserUtil.java生成token文件")
        print("运行方法：在项目中执行UserUtil的main方法")

if __name__ == "__main__":
    print("开始生成秒杀系统压测数据...")
    
    # 生成基础数据
    generate_users_csv(1000)  # 生成1000个用户
    generate_goods_csv()
    generate_miaosha_goods_csv()
    
    # 尝试转换token文件
    generate_tokens_csv()
    
    print("\n数据生成完成！")
    print("文件清单：")
    print("- users.csv: 用户基础数据（用于登录测试）")
    print("- goods.csv: 商品数据")  
    print("- miaosha_goods.csv: 秒杀商品数据")
    print("- user_tokens.csv: 用户token数据（需要先运行UserUtil生成）")