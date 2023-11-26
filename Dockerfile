# Используем базовый образ с Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию в /src
WORKDIR /src

# Устанавливаем зависимости, включая libpq-dev
RUN apt-get update \
    && apt-get install -y libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости в рабочую директорию
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

RUN python -c "import nltk; nltk.download('punkt')"

RUN python -c "import nltk; nltk.download('averaged_perceptron_tagger_ru')"

# Копируем код в контейнер
COPY . .

# Команда для запуска кода при запуске контейнера
CMD ["python", "text_similarity_engine.py", "0.1"]
