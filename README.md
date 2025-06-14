## Описание проектов

**1. Создание основного дашборда для аналитики новостной ленты**

• Описание:  добавить ключевые метрики и визуализации, которые помогут ответить на вопрос «сколько?».

• Стек: SQL, Superset.

• Результаты:

На дашборде отображены:

- Базовые аудиторные метрики (DAU, WAU, MAU).
- Распределение аудитории по возрасту и полу.
-  Ключевые события продукта (просмотры, лайки, CTR и др.).
- Добавлена фильтрация.

**2. Анализ результата внедрения новой системы рекомендаций**

• Описание: написать рекомендацию стоит ли раскатывать новый алгоритм на всех пользователей.

• Стек: Python, Scipy, Pandas, Numpy, Seaborn, SQL, GitLab.

• Результаты:

- Проведен АА-тест для оценки качества системы сплитования. 
- Проведен АВ-тест с помощью различных методов: t-тест, тест Манна-Уитни, Пуассоновский бутстреп, бакетное преобразование.
- Выявлено отрицательное влияние нового алгоритма рекомендаций на рассматриваемую метрику.

**3. Прогнозирование DAU для оптимизации нагрузки на серверы**

• Описание: спрогнозировать Daily Active Users (DAU) на месяц вперёд для предупреждения перегрузки серверов. Выбрана метрика DAU как прямой индикатор нагрузки.

• Стек: Python, Orbit (модели временных рядов), Pandas, Matplotlib

• Результаты:

— Построена модель прогнозирования с дневным разрешением (без регрессоров)
— Обеспечена точность, достаточная для планирования инфраструктуры
— Выявлены сезонные паттерны активности

**4. Построение ETL-пайплайна**
   
• Описание: построить ETL-пайплайн для автоматизации сбора данных.

• Стек: Airflow, SQL, Pithon, Pandas, Numpy.

• Результаты:

— Реализован ETL-пайпплайн для ежедневного получения метрик по нескольким срезам.

**5. Создание чат-бота для автоматизации отчетности**
   
• Описание: автоматизировать процесс создания отчета и его отправку в чат.

• Стек: SQL, Python, Pandas, Telegram, Airflow, GitLab.

• Результаты:

— Создан чат-бот, который отправляет отчеты.
—  Собраны метрики и построен дашборд для ежедневной отчетности по системе.
—  С помощью Airflow автоматизирован ежедневный сбор отчета и его отправка.
