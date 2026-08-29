FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv

# Зависимости ставятся отдельным слоем: правка кода не пересобирает их заново.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Процесс не должен работать от root даже внутри контейнера.
RUN useradd --create-home --uid 10001 app && chown -R app:app /srv
USER app

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
