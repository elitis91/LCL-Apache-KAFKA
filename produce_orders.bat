@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ======= Configuration =======
set "BOOTSTRAP=localhost:9092"
set "TOPIC=orders"
set "KAFKA_HOME=C:\kafka"
set "KAFKA_BIN=%KAFKA_HOME%\bin\windows"
set "PRODUCER=%KAFKA_BIN%\kafka-console-producer.bat"

REM 30 commandes / minute => 1 message toutes les 2 secondes
set "SLEEP_SECONDS=2"

REM Point de départ des IDs (modifiable)
set /a ID=1452

echo.
echo ===== Kafka Orders Producer Simulator =====
echo Bootstrap: %BOOTSTRAP%
echo Topic:     %TOPIC%
echo Rate:      30 msg/min (1 msg / %SLEEP_SECONDS% s)
echo Press CTRL+C to stop.
echo.

REM ======= Boucle infinie : génère 1 JSON puis l'envoie =======
:loop

REM Petite sélection pseudo-aléatoire de produits/prix/quantité
set /a R=!random! %% 6

if !R! EQU 0 (set "PRODUCT=laptop"   & set /a PRICE=500)
if !R! EQU 1 (set "PRODUCT=mouse"    & set /a PRICE=25)
if !R! EQU 2 (set "PRODUCT=keyboard" & set /a PRICE=70)
if !R! EQU 3 (set "PRODUCT=screen"   & set /a PRICE=180)
if !R! EQU 4 (set "PRODUCT=headset"  & set /a PRICE=60)
if !R! EQU 5 (set "PRODUCT=ssd"      & set /a PRICE=120)

set /a QTY=(!random! %% 4) + 1
set /a TOTAL=PRICE*QTY

REM Timestamp ISO-ish (local) : YYYY-MM-DDTHH:MM:SS
for /f "tokens=1-3 delims=/- " %%a in ("%date%") do (
  set "D1=%%a"
  set "D2=%%b"
  set "D3=%%c"
)
REM Selon la locale Windows, l'ordre peut varier; on fait simple et on garde date/time brut aussi.
set "TS=%date% %time%"

REM Construire le JSON (en string)
set "JSON={ "id_order": !ID!, "product_name": "!PRODUCT!", "nombre": !QTY!, "total_price": !TOTAL!, "ts": "!TS!" }"

echo Sending: !JSON!

REM Envoyer 1 message à Kafka
echo !JSON! | "%PRODUCER%" --bootstrap-server %BOOTSTRAP% --topic %TOPIC% >nul

set /a ID+=1

REM Pause ~2 secondes (cadence 30/min)
timeout /t %SLEEP_SECONDS% /nobreak >nul

goto :loop
