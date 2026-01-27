@echo off
title Automatizador Git - %CD%
echo === Iniciando Sincronizacion ===

:: 1. Desbloquea la seguridad del USB para esta carpeta
git config --local safe.directory "%CD%"

:: 2. Baja cambios de GitHub por si trabajaste en otra PC
echo Descargando posibles cambios de la nube...
git pull origin master

:: 3. Agrega todos los nuevos cambios
echo Preparando archivos...
git add .

:: 4. Pide al usuario un mensaje para el commit
set /p msg="Escribe que hiciste hoy: "
git commit -m "%msg%"

:: 5. Sube todo a GitHub
echo Subiendo a GitHub...
git push origin master

echo === Proceso Terminado con Exito ===
pause