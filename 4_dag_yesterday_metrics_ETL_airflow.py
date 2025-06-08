from datetime import datetime, timedelta

import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task

# подключение к бд
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student',
    'password': 'dpo_python_2020',
    'database': 'simulator_20250320'
}

# подключение к бд test
connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student-rw',
    'password': '656e2b0c9c',
    'database': 'test'
}

# параметры dag
default_args = {
    'owner': 'anna-arzhakova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 9),
}

# интервал запуска
schedule_interval = '0 12 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_yesterday_metrics():
    # запрос в feed_actions
    @task()
    def extract_feed():
        query = """
        SELECT 
            user_id,
            os,
            gender,
            age,
            countIf(action = 'view') as views,
            countIf(action = 'like') as likes,
            toDate(time) as event_date
        FROM simulator_20250320.feed_actions
        WHERE toDate(time) = yesterday()
        GROUP BY user_id, os, gender, age, event_date
        """
        return ph.read_clickhouse(query, connection=connection)
    # запрос для отправленных сообщений в message_actions
    @task()
    def extract_messages_sent():
        query = """
        SELECT 
            user_id,
            os,
            gender,
            age,
            count() as messages_sent,
            count(DISTINCT receiver_id) as users_sent,
            toDate(time) as event_date
        FROM simulator_20250320.message_actions
        WHERE toDate(time) = yesterday()
        GROUP BY user_id, os, gender, age, event_date
        """
        return ph.read_clickhouse(query, connection=connection)
    # запрос для полученных сообщений в message_actions
    @task()
    def extract_messages_received():
        query = """
        SELECT 
            receiver_id as user_id,
            os,
            gender,
            age,
            count() as messages_received,
            count(DISTINCT user_id) as users_received,
            toDate(time) as event_date
        FROM simulator_20250320.message_actions
        WHERE toDate(time) = yesterday()
        GROUP BY receiver_id, os, gender, age, event_date
        """
        return ph.read_clickhouse(query, connection=connection)
    # объединение message_actions и feed_action
    @task()
    def merge_data(feed, messages_sent, messages_received):
        # Объединяем данные по user_id, os, gender, age, event_date
        merged = feed.merge(
            messages_sent, 
            on=['user_id', 'os', 'gender', 'age', 'event_date'], 
            how='outer'
        ).fillna(0)

        merged = merged.merge(
            messages_received, 
            on=['user_id', 'os', 'gender', 'age', 'event_date'], 
            how='outer'
        ).fillna(0)

        return merged
    # агрегация по полу, возрасту и ос
    @task()
    def transform_os(merged):
        os_metrics = merged[['event_date', 'os', 'views', 'likes', 'messages_received',
                           'messages_sent', 'users_received', 'users_sent']].groupby(
            ['event_date', 'os'], as_index=False).sum().rename(columns={'os': 'dimension_value'})
        os_metrics.insert(1, 'dimension', 'os')
        return os_metrics

    @task()
    def transform_gender(merged):
        gender_metrics = merged[['event_date', 'gender', 'views', 'likes', 'messages_received',
                               'messages_sent', 'users_received', 'users_sent']].groupby(
            ['event_date', 'gender'], as_index=False).sum().rename(columns={'gender': 'dimension_value'})
        gender_metrics.insert(1, 'dimension', 'gender')
        return gender_metrics

    @task()
    def transform_age(merged):
        age_metrics = merged[['event_date', 'age', 'views', 'likes', 'messages_received',
                            'messages_sent', 'users_received', 'users_sent']].groupby(
            ['event_date', 'age'], as_index=False).sum().rename(columns={'age': 'dimension_value'})
        age_metrics.insert(1, 'dimension', 'age')
        return age_metrics
    # финальные данные записываем в таблицу clickhouse
    @task()
    def load_to_clickhouse(gender_metrics, age_metrics, os_metrics):
        final_df = pd.concat([gender_metrics, age_metrics, os_metrics])

        final_df = final_df.astype({
            'views': 'int',
            'likes': 'int',
            'messages_sent': 'int',
            'messages_received': 'int',
            'users_sent': 'int',
            'users_received': 'int'
        })

        create_table_query = """
        CREATE TABLE IF NOT EXISTS test.arzhakova_yesterday_metrics (
            event_date Date,
            dimension String,
            dimension_value String,
            views Int64,
            likes Int64,
            messages_sent Int64,
            messages_received Int64,
            users_sent Int64,
            users_received Int64
        ) ENGINE = MergeTree()
        ORDER BY (event_date, dimension, dimension_value)
        """
        ph.execute(create_table_query, connection=connection_test)

        ph.to_clickhouse(final_df, table='arzhakova_yesterday_metrics', index=False, connection=connection_test)

    # определение зависимостей
    feed = extract_feed()
    messages_sent = extract_messages_sent()
    messages_received = extract_messages_received()

    merged = merge_data(feed, messages_sent, messages_received)

    os_metrics = transform_os(merged)
    gender_metrics = transform_gender(merged)
    age_metrics = transform_age(merged)

    load_to_clickhouse(gender_metrics, age_metrics, os_metrics)

dag_yesterday_metrics = dag_yesterday_metrics()
