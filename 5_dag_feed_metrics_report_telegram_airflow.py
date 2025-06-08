from datetime import datetime, timedelta
import io
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph

from airflow.decorators import dag, task


# подключение к бд
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'user': 'student',
    'password': 'dpo_python_2020',
    'database': 'simulator_20250320'
}

def feed_metrics_report(chat_id):
    my_token = '' # токен бота
    bot = telegram.Bot(token=my_token) # получаем доступ
    #запрос в бд для вывода метрик
    q = """
    SELECT toDate(time) as day,
        count(DISTINCT user_id) as dau,
        sum(action = 'view') as views,
        sum(action = 'like') as likes,
        likes/views as ctr
    FROM simulator_20250320.feed_actions
    WHERE toDate(time) BETWEEN today()-7 and  today()-1
    GROUP BY toDate(time)
    """

    df = ph.read_clickhouse(q, connection=connection)

    #построение графика с ключевыми метриками
    df_scaled = df.copy()

    #нормализация ключевых метрик
    for col in ['dau', 'views', 'likes', 'ctr']:
        df_scaled[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())

    plt.figure(figsize=(12, 7))
    sns.set_style("whitegrid")
    
    sns.lineplot(data=df_scaled, x='day', y='dau', label='DAU', linewidth=2)
    sns.lineplot(data=df_scaled, x='day', y='views', label='Views', linewidth=2)
    sns.lineplot(data=df_scaled, x='day', y='likes', label='Likes', linewidth=2)
    sns.lineplot(data=df_scaled, x='day', y='ctr', label='CTR', linewidth=2)

    plt.title('Нормализованные ключевые метрики за предыдущие 7 дней', pad=20)
    plt.xlabel("Дата", labelpad=10)
    plt.ylabel("Нормализованное значение", labelpad=10)
    plt.legend(title='Метрики', bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.tight_layout()  # автоматическая подгонка layout
    plot_object = io.BytesIO()
    plt.savefig(plot_object,format='png')

    #отправка сообщения с графиком
    plot_object.seek(0)
    plot_object.name = 'metrics_plot.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    plot_object.close() 

    # текст с информацией о значениях ключевых метрик за предыдущий день
    msg = (f"*Отчет за {df['day'][6].strftime('%Y-%m-%d')}*\n"
           f"*DAU:* {df['dau'][6]}\n"
           f"*Просмотры:* {df['views'][6]}\n"
           f"*Лайки:* {df['likes'][6]}\n"
           f"*CTR:* {df['ctr'][6]:.2%}")  

    bot.sendMessage(chat_id=chat_id, text=msg, parse_mode="Markdown")
    

# параметры dag
default_args = {
    'owner': 'anna-arzhakova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 25),
}

# интервал запуска
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_feed_metrics_report():
    
    @task()
    def create_metrics_report():
        feed_metrics_report(chat=-938659451)

    create_metrics_report()
    
dag_feed_metrics_report = dag_feed_metrics_report()
