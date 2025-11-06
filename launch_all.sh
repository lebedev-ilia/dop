#!/bin/bash

# Скрипт для запуска всех процессов в отдельных tmux сессиях
# Использование: ./launch_all.sh

# Проверяем наличие tmux
if ! command -v tmux &> /dev/null; then
    echo "tmux не установлен. Установите: sudo apt-get install tmux (Ubuntu/Debian) или brew install tmux (macOS)"
    exit 1
fi

# Получаем абсолютный путь к проекту
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

# Функция для проверки занятости порта
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 || \
       netstat -tuln 2>/dev/null | grep -q ":$port " || \
       ss -tuln 2>/dev/null | grep -q ":$port "; then
        return 0  # Порт занят
    else
        return 1  # Порт свободен
    fi
}

# Функция для освобождения порта (автоматически, без интерактивного запроса)
free_port() {
    local port=$1
    local pid
    
    # Пытаемся найти процесс через lsof
    if command -v lsof &> /dev/null; then
        pid=$(lsof -ti :$port 2>/dev/null)
    # Или через fuser
    elif command -v fuser &> /dev/null; then
        pid=$(fuser $port/tcp 2>/dev/null | awk '{print $1}')
    # Или через ss
    elif command -v ss &> /dev/null; then
        pid=$(ss -lptn "sport = :$port" 2>/dev/null | grep -oP 'pid=\K\d+')
    fi
    
    if [ ! -z "$pid" ]; then
        echo "Найден процесс на порту $port (PID: $pid)"
        # Проверяем, не наш ли это процесс в tmux сессии
        if tmux has-session -t "prometheus" 2>/dev/null; then
            echo "Останавливаем старую tmux сессию 'prometheus'..."
            tmux kill-session -t "prometheus" 2>/dev/null
            sleep 2
        fi
        # Пытаемся убить процесс
        if kill $pid 2>/dev/null; then
            sleep 1
            echo "✓ Порт $port освобожден"
            return 0
        elif sudo kill $pid 2>/dev/null; then
            sleep 1
            echo "✓ Порт $port освобожден (с sudo)"
            return 0
        else
            echo "✗ Не удалось остановить процесс $pid"
            return 1
        fi
    fi
    
    return 1
}

# Функция для создания/переключения на tmux сессию
launch_in_tmux() {
    local session_name=$1
    local command=$2
    local working_dir=$3
    
    # Проверяем, существует ли сессия
    if tmux has-session -t "$session_name" 2>/dev/null; then
        echo "Сессия '$session_name' уже существует. Переключитесь на неё: tmux attach -t $session_name"
        return 1
    else
        # Создаем новую сессию в detached режиме
        tmux new-session -d -s "$session_name" -c "$working_dir"
        # Запускаем команду
        tmux send-keys -t "$session_name" "$command" C-m
        echo "✓ Запущена сессия '$session_name'"
        return 0
    fi
}

echo "Запуск всех процессов в tmux сессиях..."
echo ""

# 1. yt-dlp метрики сервер
launch_in_tmux "ytdlp-metrics" \
    "python _yt_dlp/_yt_dlp_metrics.py --port 8001 --host 0.0.0.0 --results-dir _yt_dlp/.results" \
    "$PROJECT_ROOT"

# 2. Prometheus
if check_port 9090; then
    echo "⚠ Порт 9090 занят. Пытаемся освободить..."
    if free_port 9090; then
        launch_in_tmux "prometheus" \
            "prometheus --config.file=\"prometheus.yml\" --web.listen-address=\":9090\"" \
            "$PROJECT_ROOT"
    else
        echo "✗ Не удалось освободить порт 9090. Пропускаем запуск Prometheus."
        echo "  Вы можете остановить процесс вручную: lsof -ti :9090 | xargs kill"
    fi
else
    launch_in_tmux "prometheus" \
        "prometheus --config.file=\"prometheus.yml\" --web.listen-address=\":9090\"" \
        "$PROJECT_ROOT"
fi

# 3. Cloudflared
launch_in_tmux "cloudflared" \
    "cloudflared tunnel --url http://localhost:9090" \
    "$PROJECT_ROOT"

# 4. main_yt_dlp_hf.py
launch_in_tmux "ytdlp-hf" \
    "python _yt_dlp/main_yt_dlp_hf.py" \
    "$PROJECT_ROOT"

echo ""
echo "Все процессы запущены!"
echo ""
echo "Полезные команды:"
echo "  tmux list-sessions              - список всех сессий"
echo "  tmux attach -t <session_name>   - подключиться к сессии"
echo "  tmux kill-session -t <session_name> - остановить сессию"
echo "  tmux kill-server                - остановить все сессии"
echo ""
echo "Для просмотра вывода:"
echo "  tmux attach -t ytdlp-metrics    - метрики yt-dlp"
echo "  tmux attach -t prometheus       - Prometheus"
echo "  tmux attach -t cloudflared      - Cloudflared"
echo "  tmux attach -t ytdlp-hf         - HuggingFace uploader"
echo ""
echo "Для main_yt_dlp.py (если ещё не запущен):"
echo "  tmux new-session -s ytdlp-main -c \"$PROJECT_ROOT\""
echo "  # Затем в tmux: python _yt_dlp/main_yt_dlp.py"

