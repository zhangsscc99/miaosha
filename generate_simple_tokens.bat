@echo off
chcp 65001 >nul
echo Generating simple tokens for load testing...

REM Create a basic tokens.txt file for testing
echo 13000000000,test_token_0 > tokens.txt
echo 13000000001,test_token_1 >> tokens.txt
echo 13000000002,test_token_2 >> tokens.txt
echo 13000000003,test_token_3 >> tokens.txt
echo 13000000004,test_token_4 >> tokens.txt
echo 13000000005,test_token_5 >> tokens.txt
echo 13000000006,test_token_6 >> tokens.txt
echo 13000000007,test_token_7 >> tokens.txt
echo 13000000008,test_token_8 >> tokens.txt
echo 13000000009,test_token_9 >> tokens.txt

REM Generate more tokens using loop
for /L %%i in (10,1,99) do (
    echo 1300000000%%i,test_token_%%i >> tokens.txt
)

echo Generated 100 test tokens in tokens.txt
echo Note: These are placeholder tokens for testing JMeter script structure
echo For real load testing, you should generate actual tokens from the application

pause