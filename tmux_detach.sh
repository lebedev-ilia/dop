#!/bin/bash

# Утилита для отключения от tmux сессии
# Использование: ./tmux_detach.sh [session_name]

if [ -z "$1" ]; then
    # Если сессия не указана, пытаемся отключиться от текущей
    if [ -n "$TMUX" ]; then
        echo "Отключение от текущей tmux сессии..."
        tmux detach
    else
        echo "Использование: $0 [session_name]"
        echo "Примеры:"
        echo "  $0                - отключиться от текущей сессии (если внутри tmux)"
        echo "  $0 prometheus     - отключиться от сессии prometheus"
        echo ""
        echo "Или используйте внутри tmux:"
        echo "  :detach           - ввести команду detach в tmux"
        exit 1
    fi
else
    SESSION_NAME=$1
    if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        echo "Отключение от сессии '$SESSION_NAME'..."
        tmux detach -s "$SESSION_NAME"
    else
        echo "Сессия '$SESSION_NAME' не найдена"
        echo "Доступные сессии:"
        tmux list-sessions 2>/dev/null || echo "Нет активных сессий"
        exit 1
    fi
fi

