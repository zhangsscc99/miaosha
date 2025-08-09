@echo off
chcp 65001 >nul
REM 秒杀系统压测启动脚本 (Windows版本)

echo ========== 秒杀系统压测启动脚本 ==========

REM 检查JMeter是否安装
where jmeter >nul 2>&1
if %errorlevel% neq 0 (
    echo 错误：JMeter未安装或不在PATH中
    echo 请先安装JMeter并添加到系统PATH
    pause
    exit /b 1
)

REM 检查测试文件是否存在
if not exist "miaosha-performance-test.jmx" (
    echo 错误：未找到JMeter测试脚本 miaosha-performance-test.jmx
    pause
    exit /b 1
)

if not exist "users.csv" (
    echo 错误：未找到用户数据文件 users.csv
    echo 请先运行 python generate_test_data.py 生成测试数据
    pause
    exit /b 1
)

REM 创建结果目录
if not exist "results" mkdir results
for /f "tokens=*" %%i in ('powershell -command "Get-Date -Format 'yyyyMMdd_HHmmss'"') do set TIMESTAMP=%%i
set RESULT_DIR=results\test_%TIMESTAMP%
mkdir "%RESULT_DIR%"

echo 测试结果将保存到: %RESULT_DIR%

REM 压测场景选择
echo.
echo 请选择压测场景：
echo 1. 基准性能测试 (100并发, 10分钟)
echo 2. 秒杀压力测试 (1000并发, 2分钟)
echo 3. 混合场景测试 (500并发, 20分钟)
echo 4. 自定义参数测试

set /p choice=请输入选择 (1-4): 

if "%choice%"=="1" (
    set THREADS=100
    set DURATION=600
    set SCENARIO=基准性能测试
) else if "%choice%"=="2" (
    set THREADS=1000
    set DURATION=120
    set SCENARIO=秒杀压力测试
) else if "%choice%"=="3" (
    set THREADS=500
    set DURATION=1200
    set SCENARIO=混合场景测试
) else if "%choice%"=="4" (
    set /p THREADS=请输入并发用户数: 
    set /p DURATION=请输入测试时长(秒): 
    set SCENARIO=自定义测试
) else (
    echo 无效选择，使用默认基准测试
    set THREADS=100
    set DURATION=600
    set SCENARIO=基准性能测试
)

echo.
echo ========== 测试配置 ==========
echo 场景: %SCENARIO%
echo 并发用户数: %THREADS%
echo 测试时长: %DURATION%秒
echo 开始时间: %date% %time%

REM 检查应用是否启动
echo.
echo 检查应用服务状态...
curl -s http://localhost:8080/goods/to_list >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ 应用服务正常运行
) else (
    echo ✗ 应用服务未启动或无法访问
    echo 请先启动应用: mvn spring-boot:run
    set /p continue_test=是否继续运行测试? (y/n): 
    if not "!continue_test!"=="y" exit /b 1
)

REM 复制测试配置
copy "miaosha-performance-test.jmx" "%RESULT_DIR%\test_config.jmx"

REM 执行JMeter测试
echo.
echo ========== 开始压力测试 ==========
echo 测试进行中，请耐心等待...

jmeter -n -t "%RESULT_DIR%\test_config.jmx" -l "%RESULT_DIR%\results.jtl" -e -o "%RESULT_DIR%\html_report" -Jthreads=%THREADS% -Jduration=%DURATION% -Jhost=localhost -Jport=8080

REM 检查测试是否成功
if %errorlevel% equ 0 (
    echo.
    echo ========== 测试完成 ==========
    echo 结束时间: %date% %time%
    echo 测试结果保存在: %RESULT_DIR%
    
    REM 显示基本统计信息
    if exist "%RESULT_DIR%\results.jtl" (
        echo.
        echo ========== 快速统计 ==========
        
        REM 统计总请求数
        for /f %%i in ('powershell -command "(Get-Content '%RESULT_DIR%\results.jtl' | Measure-Object -Line).Lines - 1"') do set TOTAL_REQUESTS=%%i
        echo 总请求数: %TOTAL_REQUESTS%
        
        echo.
        echo 详细报告请查看: %RESULT_DIR%\html_report\index.html
    )
    
    REM 询问是否打开报告
    set /p open_report=是否打开HTML报告? (y/n): 
    if "%open_report%"=="y" (
        start "" "%RESULT_DIR%\html_report\index.html"
    )
    
) else (
    echo.
    echo ========== 测试失败 ==========
    echo 请检查JMeter配置和应用状态
    pause
    exit /b 1
)

echo.
echo ========== 后续建议 ==========
echo 1. 查看详细HTML报告分析性能瓶颈
echo 2. 检查应用日志和系统资源使用情况
echo 3. 根据测试结果调整系统配置
echo 4. 可以尝试不同并发数进行对比测试

pause