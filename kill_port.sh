#!/bin/bash

# Утилита для остановки процесса на указанном порту
# Использование: ./kill_port.sh <port>

if [ -z "$1" ]; then
    echo "Использование: $0 <port>"
    echo "Пример: $0 9090"
    exit 1
fi

PORT=$1
PID=""

# Пытаемся найти процесс
if command -v lsof &> /dev/null; then
    PID=$(lsof -ti :$PORT 2>/dev/null)
elif command -v fuser &> /dev/null; then
    PID=$(fuser $PORT/tcp 2>/dev/null | awk '{print $1}')
elif command -v ss &> /dev/null; then
    PID=$(ss -lptn "sport = :$PORT" 2>/dev/null | grep -oP 'pid=\K\d+')
fi

if [ -z "$PID" ]; then
    echo "Порт $PORT не занят"
    exit 0
fi

echo "Найден процесс на порту $PORT: PID=$PID"
echo "Команда процесса: $(ps -p $PID -o comm= 2>/dev/null || echo 'неизвестно')"

# Пытаемся остановить
if kill $PID 2>/dev/null; then
    echo "✓ Процесс $PID остановлен"
    sleep 1
    
    # Проверяем, освободился ли порт
    if command -v lsof &> /dev/null; then
        if lsof -ti :$PORT >/dev/null 2>&1; then
            echo "⚠ Порт $PORT все еще занят. Попытка принудительной остановки..."
            kill -9 $PID 2>/dev/null || sudo kill -9 $PID 2>/dev/null
        fi
    fi
    exit 0
else
    echo "✗ Не удалось остановить процесс $PID"
    echo "Попытка с sudo..."
    if sudo kill $PID 2>/dev/null; then
        echo "✓ Процесс $PID остановлен (с sudo)"
        exit 0
    else
        echo "✗ Не удалось остановить процесс даже с sudo"
        exit 1
    fi
fi

