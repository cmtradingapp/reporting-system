@echo off
title SSH Tunnel — MT5Node (Redis + API + PG)
echo ============================================
echo  SSH Tunnel to MT5Node Server (plink)
echo  109.199.112.72 (root)
echo ============================================
echo.
echo  Local 16379  -^>  Remote 6379  (Redis)
echo  Local 18080  -^>  Remote 8080  (Positions API)
echo  Local 15432  -^>  Remote 5432  (PostgreSQL)
echo.
echo  Press Ctrl+C to close the tunnel.
echo ============================================
echo.

plink -ssh -i "C:\Users\elise.i\.ssh\claude_key.ppk" ^
    -N -batch ^
    -L 16379:127.0.0.1:6379 ^
    -L 18080:127.0.0.1:8080 ^
    -L 15432:127.0.0.1:5432 ^
    root@109.199.112.72

echo.
echo Tunnel closed.
pause
