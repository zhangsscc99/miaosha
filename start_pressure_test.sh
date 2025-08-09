#!/bin/bash
# 秒杀系统压测启动脚本

echo "========== 秒杀系统压测启动脚本 =========="

# 检查JMeter是否安装
if ! command -v jmeter &> /dev/null; then
    echo "错误：JMeter未安装或不在PATH中"
    echo "请先安装JMeter: https://jmeter.apache.org/download_jmeter.cgi"
    exit 1
fi

# 检查测试文件是否存在
if [ ! -f "miaosha-performance-test.jmx" ]; then
    echo "错误：未找到JMeter测试脚本 miaosha-performance-test.jmx"
    exit 1
fi

if [ ! -f "users.csv" ]; then
    echo "错误：未找到用户数据文件 users.csv"
    echo "请先运行 python generate_test_data.py 生成测试数据"
    exit 1
fi

# 创建结果目录
mkdir -p results
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_DIR="results/test_$TIMESTAMP"
mkdir -p "$RESULT_DIR"

echo "测试结果将保存到: $RESULT_DIR"

# 压测场景选择
echo ""
echo "请选择压测场景："
echo "1. 基准性能测试 (100并发, 10分钟)"
echo "2. 秒杀压力测试 (1000并发, 2分钟)" 
echo "3. 混合场景测试 (500并发, 20分钟)"
echo "4. 自定义参数测试"

read -p "请输入选择 (1-4): " choice

case $choice in
    1)
        THREADS=100
        DURATION=600
        SCENARIO="基准性能测试"
        ;;
    2)
        THREADS=1000
        DURATION=120
        SCENARIO="秒杀压力测试"
        ;;
    3)
        THREADS=500
        DURATION=1200
        SCENARIO="混合场景测试"
        ;;
    4)
        read -p "请输入并发用户数: " THREADS
        read -p "请输入测试时长(秒): " DURATION
        SCENARIO="自定义测试"
        ;;
    *)
        echo "无效选择，使用默认基准测试"
        THREADS=100
        DURATION=600
        SCENARIO="基准性能测试"
        ;;
esac

echo ""
echo "========== 测试配置 =========="
echo "场景: $SCENARIO"
echo "并发用户数: $THREADS"
echo "测试时长: ${DURATION}秒"
echo "开始时间: $(date)"

# 检查应用是否启动
echo ""
echo "检查应用服务状态..."
if curl -s http://localhost:8080/goods/to_list > /dev/null; then
    echo "✓ 应用服务正常运行"
else
    echo "✗ 应用服务未启动或无法访问"
    echo "请先启动应用: mvn spring-boot:run"
    read -p "是否继续运行测试? (y/n): " continue_test
    if [[ $continue_test != "y" ]]; then
        exit 1
    fi
fi

# 执行JMeter测试
echo ""
echo "========== 开始压力测试 =========="
echo "测试进行中，请耐心等待..."

# 修改JMeter脚本中的参数（如果需要）
cp miaosha-performance-test.jmx "$RESULT_DIR/test_config.jmx"

# 运行JMeter测试
jmeter -n -t "$RESULT_DIR/test_config.jmx" \
       -l "$RESULT_DIR/results.jtl" \
       -e -o "$RESULT_DIR/html_report" \
       -Jthreads="$THREADS" \
       -Jduration="$DURATION" \
       -Jhost="localhost" \
       -Jport="8080"

# 检查测试是否成功
if [ $? -eq 0 ]; then
    echo ""
    echo "========== 测试完成 =========="
    echo "结束时间: $(date)"
    echo "测试结果保存在: $RESULT_DIR"
    
    # 显示基本统计信息
    if [ -f "$RESULT_DIR/results.jtl" ]; then
        echo ""
        echo "========== 快速统计 =========="
        
        # 统计总请求数
        TOTAL_REQUESTS=$(tail -n +2 "$RESULT_DIR/results.jtl" | wc -l)
        echo "总请求数: $TOTAL_REQUESTS"
        
        # 统计成功率
        SUCCESS_COUNT=$(tail -n +2 "$RESULT_DIR/results.jtl" | awk -F',' '$8=="true" {count++} END {print count+0}')
        SUCCESS_RATE=$(echo "scale=2; $SUCCESS_COUNT * 100 / $TOTAL_REQUESTS" | bc -l 2>/dev/null || echo "N/A")
        echo "成功率: ${SUCCESS_RATE}%"
        
        # 平均响应时间
        AVG_RESPONSE=$(tail -n +2 "$RESULT_DIR/results.jtl" | awk -F',' '{sum+=$2; count++} END {print sum/count}')
        echo "平均响应时间: ${AVG_RESPONSE}ms"
        
        echo ""
        echo "详细报告请查看: $RESULT_DIR/html_report/index.html"
    fi
    
    # 打开报告（如果是图形环境）
    if command -v xdg-open &> /dev/null; then
        read -p "是否打开HTML报告? (y/n): " open_report
        if [[ $open_report == "y" ]]; then
            xdg-open "$RESULT_DIR/html_report/index.html"
        fi
    fi
    
else
    echo ""
    echo "========== 测试失败 =========="
    echo "请检查JMeter配置和应用状态"
    exit 1
fi

echo ""
echo "========== 后续建议 =========="
echo "1. 查看详细HTML报告分析性能瓶颈"  
echo "2. 检查应用日志和系统资源使用情况"
echo "3. 根据测试结果调整系统配置"
echo "4. 可以尝试不同并发数进行对比测试"