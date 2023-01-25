1.В файле docker-compose.yaml находится команды для поднятия mssql server в докере
2. Dockerfile - это файл для поднятия pyspark на докере
Для поднятия pyspark на докере воспользоваться следующими командами.
2.1 docker build -t pyspark --build-arg PYTHON_VERSION=3.7.10 --build-arg IMAGE=buster .
2.2 docker run -it pyspark
3.Для запуска модели надо запустить train.py и результаты модели автоматически сохраняются в БД.
4. Для аутентификации к БД параметры находятся в config.py а так передаются зашифрованные параметры
