# Текстовый анализатор темы
## Что делает?
- Определяет принадлежность теме полученного текста
- Возвращает процент принадлежности текста выбранной теме
## Как настроить?
1) Составить тексты, которые принадлежат выбранной теме
2) Запустить файл docker-compose `docker-compose up` , если нужно другое количество экземпляров тогда изменить в docker-compose textanalyser.deploy.replicas на то количество которое вам нужно
3) Подключить ваш сервис к очереди rabbitmq (смотреть документацию в файле asyncapi.yaml)
4) Передать начальный набор текстов в анализатор с весами 1 или 0, если текст точно принадлежит теме или не принадлежит соответственно
## Как использовать?
В качестве payload в мессендж брокер находящийся на порту 5672 передается json (смотреть пример в документации) c полем label выставленным "?", после завершения обработки уровень принадлежности текста и дополнительная служебная информация выводятся на экран, а в базе находящейся на порте 5432 появляется новая запись, до обработки сообщения находятся в очередь, мониторить очередь можно через вебинтерфейс находящийся на порте 15672
