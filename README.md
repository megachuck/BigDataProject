# Big Data 

Этот репозиторий содержит **Dockerfile** от [apache-airflow](https://github.com/apache/incubator-airflow) для [Docker](https://www.docker.com/)'s [автоматической сборки](https://registry.hub.docker.com/u/puckel/docker-airflow/) опубликованной в общедоступном реестре [Docker Hub Registry](https://registry.hub.docker.com/).

## Информация

* Основано на Python (3.7-slim-buster) официальное изображение [python:3.7-slim-buster](https://hub.docker.com/_/python/) и использует официальный [Postgres](https://hub.docker.com/_/postgres/)
* Скачать [Docker](https://www.docker.com/)
* Скачать [Docker Compose](https://docs.docker.com/compose/install/)

## Предустановка

Для полноценного использования проекта вам необходимо скачать датасеты, поменять их названия, как написано ниже и поместить их в папку datasets.
* Скачать [Traffic_Volume_Counts.csv](https://data.cityofnewyork.us/Transportation/Traffic-Volume-Counts/btm5-ppia)
* Скачать [Weather.csv](https://www.kaggle.com/datasets/meinertsen/new-york-city-taxi-trip-hourly-weather-data?select=Weather.csv)
* Скачать [yellow_tripdata_2015-01.csv](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data?select=yellow_tripdata_2015-01.csv)
* Скачать [yellow_tripdata_2016-01.csv](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data?select=yellow_tripdata_2016-01.csv)
* Скачать [yellow_tripdata_2016-02.csv](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data?select=yellow_tripdata_2016-02.csv)
* Скачать [yellow_tripdata_2016-03.csv](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data?select=yellow_tripdata_2016-03.csv)

## Использование

Проект запускается через **LocalExecutor** :

    docker-compose up -d

Пароль для входа

| Пользователь        | Пароль    |
|---------------------|-----------|
| `airflow`           | `airflow` |

Если вы хотите использовать специальный запрос, убедитесь, что вы настроили подключения:
Перейдите в Admin -> Connections и отредактируйте "postgres_default", установите эти значения (эквивалентно значениям в airflow.cfg/docker-compose.yml) :

| Переменная          | Изначальное значение |
|---------------------|----------------------|
| `POSTGRES_HOST`     | `postgres`           | 
| `POSTGRES_PORT`     | `5432`               | 
| `POSTGRES_USER`     | `postgres`           |
| `POSTGRES_PASSWORD` | `postgres`           |
| `POSTGRES_DB`       | `airflow`            |

## Ссылка пользовательского интерфейса

- Airflow: [localhost:8080](http://localhost:8080/)

## Модель

Изначально, модель машинного обучения `Ridge` обучается на датасете `yellow_tripdata.csv`.\
В этом блоке кода создаются:
1. Pipeline из нормализации тренировочных значений и самой Ridge модели
2. Param_grid из перебора 1050 различных комбинаций гиперпамаретров линейной модели
3. Search - построенный объект класса `GridSearchCV` для самого обучения.
```python
pipe = Pipeline([
    ('scaler', StandardScaler()),
    ('ridge_model', Ridge())
])

param_grid = {
    'ridge_model__alpha': np.linspace(1, 200, 50),
    'ridge_model__max_iter': [250, 500, 1000]
}

splitter = KFold(n_splits=7)

search = GridSearchCV(
    estimator=pipe,
    param_grid=param_grid,
    cv=splitter,
    scoring='neg_mean_squared_error',
    verbose=2
)
```
Последующий **Fine tuning** модели будет происходить с этим же GridSearch'ем

---

## Airflow DAG ftmodel.py

При запуске данного дага происходит дообучение модели\
К сожалению, мы <ins>не смогли</ins> сделать выборочный fine tuning по последним введённым данным, поэтому модель будет обучаться снова. Код полностью рабочий, однако, по достижении
```python
model.best_estimator_.fit(X, y)
```
обучение модели займёт много ресурсов и времени. Есть шанс выйти в `ООМ`, <ins>запускайте с осторожностью</ins>!


## Нужна помощь?

Fork, improve и Pull request.
