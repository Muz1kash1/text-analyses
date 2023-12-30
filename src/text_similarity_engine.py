# Импорт необходимых библиотек и модулей
import json
import logging
import os
import re
import tempfile
import uuid
from concurrent.futures import ProcessPoolExecutor


import pika
import pymorphy2
from database import Database, ReferenceSample
from dotenv import load_dotenv
from nltk import pos_tag, sent_tokenize, word_tokenize


def pymorphy2_311_hotfix():
    from inspect import getfullargspec

    from pymorphy2.units.base import BaseAnalyzerUnit

    def _get_param_names_311(klass):
        if klass.__init__ is object.__init__:
            return []
        args = getfullargspec(klass.__init__).args
        return sorted(args[1:])

    setattr(BaseAnalyzerUnit, "_get_param_names", _get_param_names_311)

pymorphy2_311_hotfix()
morph = pymorphy2.MorphAnalyzer()

def normalize_word(word: str) -> str:
    """
    Нормализует слово, приводя его к нормальной (базовой) форме с использованием морфологического анализатора.

    Параметры:
    - word (str): Слово, которое требуется нормализовать.

    Возвращает:
    - str: Нормализованная форма слова.

    Пример использования:
    >>> normalize_word("бежавший")
    "бежать"
    """
    # Инициализация морфологического анализатора

    # Получение разбора слова и выбор первого варианта (наиболее вероятного)
    parsed_word = morph.parse(word)[0]

    # Возвращение нормальной формы слова
    return parsed_word[2]


def split_text_into_fragments(text: str, max_series=5):
    """
    Разделяет текст на фрагменты, используя предложения как базовые единицы.

    Параметры:
    - text (str): Исходный текст, который нужно разделить на фрагменты.
    - max_series (int): Максимальное количество предложений в одном фрагменте.

    Возвращает:
    - list: Список фрагментов текста, где каждый фрагмент представлен списком предложений.

    Пример использования:
    >>> split_text_into_fragments("Это предложение. И это еще одно предложение.", max_series=2)
    [['Это предложение.', 'И это еще одно предложение.']]
    """
    # Инициализация пустого списка для хранения фрагментов
    fragments = []

    # Токенизация предложений в тексте
    sentences = sent_tokenize(text)

    # Расчет количества фрагментов на основе максимального количества предложений в фрагменте
    series = (len(sentences) - len(fragments) * max_series) // max_series

    # Создание фрагментов
    while series:
        index = len(fragments) * max_series
        fragments.append(sentences[index : index + max_series])
        series = (len(sentences) - len(fragments) * max_series) // max_series

    # Обработка оставшихся предложений после создания максимального числа фрагментов
    last_index = len(sentences) // max_series
    if len(sentences) % max_series:
        fragments.append(sentences[last_index * max_series :])

    # Возвращение списка фрагментов
    return fragments


def extract_first_signs(fragment: list[str]) -> list[list[str]]:
    """
    Извлекает признаки первого уровня из фрагмента текста.

    Параметры:
    - fragment (list): Список предложений, представляющих фрагмент текста.

    Возвращает:
    - list: Список признаков первого уровня, где каждый признак представлен списком нормализованных слов.

    Пример использования:
    >>> extract_first_signs(['Это предложение.', 'И это еще одно предложение.'])
    [['это', 'предложение'], ['и', 'это', 'еще', 'одно', 'предложение']]
    """
    # Инициализация пустого списка для хранения признаков
    signs = []

    # Обработка каждого предложения в фрагменте
    for sentence in fragment:
        # Токенизация слов в предложении
        word_list = word_tokenize(sentence)

        # Очистка слов от символов, не являющихся кириллическими буквами
        cleaned_word_list = [
            re.sub("[^а-яё]", "", word.lower()) for word in word_list if len(word) > 2
        ]

        # Удаление пустых строк из списка слов
        cleaned_word_list = list(filter(("").__ne__, cleaned_word_list))

        # Нормализация слов и добавление их в список признаков
        signs.append(list(map(normalize_word, cleaned_word_list)))

    # Возвращение списка признаков первого уровня
    return signs


def extract_second_signs(signs_list: list[list[str]]) -> list[list[str]]:
    """
    Извлекает признаки второго уровня из списка признаков первого уровня.

    Параметры:
    - signs_list (list): Список признаков первого уровня.

    Возвращает:
    - list: Список признаков второго уровня, где каждый признак представлен списком существительных и глаголов.

    Пример использования:
    >>> extract_second_signs([['это', 'предложение'], ['и', 'это', 'еще', 'одно', 'предложение']])
    [['предложение'], ['предложение']]
    """

    # Внутренняя функция для получения существительных и глаголов из набора признаков
    def get_noun_verb_signs(sign_set):
        parts_of_speech = pos_tag(sign_set, lang="rus")
        noun_verb_signs = [
            part[0] for part in parts_of_speech if part[1][0] in ["S", "V"]
        ]
        return noun_verb_signs

    # Инициализация пустого списка для хранения признаков второго уровня
    signs = []

    # Расчет длины списка признаков первого уровня
    length = len(signs_list)

    # Обработка пар признаков первого уровня
    for l in range(length // 2):
        rough_list = signs_list[2 * l] + signs_list[2 * l + 1]
        signs.append(get_noun_verb_signs(rough_list))

    # Обработка оставшегося непарного признака первого уровня
    if length % 2:
        signs.append(get_noun_verb_signs(signs_list[-1]))

    # Возвращение списка признаков второго уровня
    return signs


def extract_third_signs(signs_list: list[list[str]]) -> list[list[str]]:
    """
    Извлекает признаки третьего уровня из списка признаков второго уровня.

    Параметры:
    - signs_list (list): Список признаков второго уровня.

    Возвращает:
    - list: Список признаков третьего уровня, где каждый признак представлен списком существительных.

    Пример использования:
    >>> extract_third_signs([['предложение'], ['предложение']])
    [['предложение']]
    """

    # Внутренняя функция для получения существительных из набора признаков
    def get_nouns(sign_set):
        parts_of_speech = pos_tag(sign_set, lang="rus")
        noun_signs = [part[0] for part in parts_of_speech if part[1][0] == "S"]
        return noun_signs

    # Инициализация пустого списка для хранения признаков третьего уровня
    signs = []

    # Расчет длины списка признаков второго уровня
    length = len(signs_list)

    # Обработка троек признаков второго уровня
    for l in range(length // 3):
        rough_list = signs_list[3 * l] + signs_list[3 * l + 1] + signs_list[3 * l + 2]
        signs.append(get_nouns(rough_list))

    # Обработка оставшихся неполных троек признаков второго уровня
    if length % 3 == 2:
        signs.append(get_nouns(signs_list[-2] + signs_list[-1]))
    elif length % 3 == 1:
        signs.append(get_nouns(signs_list[-1]))

    return signs
    # # Обработка оставшихся неполных троек признаков второго уровня
    # if length % 3:
    #     signs.append(get_nouns(signs_list[-2] + signs_list[-1]))
    #
    # # Возвращение списка признаков третьего уровня
    # return signs


def generate_signatures(fragment: list[str]) -> list[list[list[str]]]:
    """
    Генерирует сигнатуры для фрагмента текста, используя функции извлечения признаков разных уровней.

    Параметры:
    - fragment (list): Список предложений, представляющих фрагмент текста.

    Возвращает:
    - list: Список, содержащий три уровня сигнатур для данного фрагмента.

    Пример использования:
    >>> generate_signatures(['Это предложение.', 'И это еще одно предложение.'])
    [
        [['это', 'предложение'], ['и', 'это', 'еще', 'одно', 'предложение']],
        [['предложение'], ['предложение']],
        [['предложение']]
    ]
    """
    # Извлечение признаков разных уровней для данного фрагмента
    first_signs_list = extract_first_signs(fragment)
    second_signs_list = extract_second_signs(first_signs_list)
    third_signs_list = extract_third_signs(first_signs_list)

    # Возвращение списка, содержащего три уровня сигнатур
    return [first_signs_list, second_signs_list, third_signs_list]


def compare_signatures(signature1, signature2):
    """
    Сравнивает две сигнатуры и возвращает вес совпадающих признаков и общее количество признаков.

    Параметры:
    - signature1 (list): первая сигнатура.
    - signature2 (list): вторая сигнатура.

    Возвращает:
    - list: Список, содержащий вес совпадающих признаков и общее количество признаков.

    Пример использования:
    >>> compare_signatures(
    ...     [['это', 'предложение'], ['и', 'это', 'еще', 'одно', 'предложение']],
    ...     [['предложение'], ['предложение']]
    ... )
    [2, 3]
    """
    # Нахождение общих признаков между двумя сигнатурами
    common_signs = [s1 for s1 in signature1 if s1 in signature2]

    # Расчет веса совпадающих признаков и общего количества признаков
    weight = len(common_signs)
    total_signs = min(len(signature1), len(signature2))

    # Возвращение списка с весом и общим количеством признаков
    return [weight, total_signs]


def get_text_id(fragment_id, text_id_length=6):
    """
    Получает идентификатор текста из идентификатора фрагмента.

    Параметры:
    - fragment_id (str): Идентификатор фрагмента текста, включающий идентификатор текста и порядковый номер фрагмента.
    - text_id_length (int): Длина идентификатора текста.

    Возвращает:
    - str: Идентификатор текста.

    Пример использования:
    >>> get_text_id("123456_001", text_id_length=6)
    '123456'
    """
    # Возвращение подстроки из идентификатора фрагмента, представляющей идентификатор текста
    return fragment_id[:text_id_length]


def update_dictionary(sign_list, etalon_fragment, fragment_id, dictionary: dict):
    """
    Обновляет словарь весов для фрагмента текста на основе сравнения признаков с эталонным фрагментом.

    Параметры:
    - sign_list (list): Список признаков для данного фрагмента текста.
    - etalon_fragment (list): Эталонный фрагмент текста, представленный списком признаков.
    - fragment_id (str): Идентификатор фрагмента текста.
    - dictionary (dict): Словарь весов для фрагментов текста.

    Возвращает:
    - dict: Обновленный словарь весов для фрагментов текста.

    Пример использования:
    >>> update_dictionary(
    ...     [['предложение'], ['предложение']],
    ...     [['это', 'предложение'], ['и', 'это', 'еще', 'одно', 'предложение']],
    ...     '123456_001',
    ...     {'123456_001': [2, 3]}
    ... )
    {'123456_001': [2, 3, 2, 3]}
    """
    # Получение списка весов для каждого признака относительно эталонного фрагмента
    # составляем список самых высоких соотношений веса к количеству признаков с при сравнении всех

    max_pair = compare_signatures(sign_list[0], etalon_fragment[0])
    max_weight = max_pair[0] / max_pair[1]
    for sign in sign_list[1:]:
        for etalon in etalon_fragment[1:]:
            current_pair = compare_signatures(sign, etalon)
            current_weight = current_pair[0] / current_pair[1]
            if max_weight < current_weight:
                max_weight = current_weight

    max_weight = 1 if max_weight > 1 else max_weight 
    if fragment_id in dictionary.keys():
        dictionary[fragment_id] = max(
            max_weight, dictionary[fragment_id]
        )
    else:
        dictionary[fragment_id] = max_weight

    return dictionary

    # weights_list = [
    #     sorted(
    #         [compare_signatures(sign, etalon_sign) for etalon_sign in etalon_fragment],
    #         key=lambda x: x[0] / x[1],
    #     )[-1]
    #     for sign in sign_list
    # ]
    #
    # # Обновление словаря весов для данного фрагмента текста
    # dictionary[fragment_id] = sorted(weights_list, key=lambda x: x[0] / x[1])[-1]
    #
    # # Возвращение обновленного словаря
    # return dictionary


class InputData:
    def __init__(self, id: uuid.UUID, text: str, label: str):
        self.id = id
        self.text = text
        self.label = label


def main_check(input_filename, db: Database, similarity_border=0.1, max_series=5):
    """
    Основная функция для проверки схожести фрагментов текста с эталонами и обновления базы данных.

    Параметры:
    - input_filename (str): Имя файла с входными данными в формате JSON.
    - db (str): Обертка над Postgres клиентом.
    - similarity_border (float): Порог схожести для определения, является ли фрагмент текста целевым.
    - max_series (int): Максимальное количество предложений в одном фрагменте текста.
    - id_legend (list): Список, содержащий два элемента - длину идентификатора текста и порядкового номера фрагмента.

    Возвращает:
    - tuple: Кортеж из двух элементов:
        1. dict: Словарь, содержащий тексты, для которых были найдены целевые фрагменты.
        2. dict: Словарь, содержащий целевые фрагменты текста и их веса.

    Пример использования:
    >>> main_check('input_data.json', 'database.json', similarity_border=0.1, max_series=5, id_legend=[6, 3])
    ({'123456': 'Это текст'}, {'123456_001': [['Это предложение.'], 0.8]})
    """
    # Чтение входных данных из файла JSON
    texts_data: list[InputData] = []
    with open(input_filename, "r", encoding="utf-8") as read_file:
        json_data = json.load(read_file)
        for item in json_data:
            texts_data.append(InputData(uuid.uuid4(), item["text"], item["label"]))

    # Чтение данных эталонов из файла JSON
    etalons_data = db.get_reference_samples()

    # Инициализация словарей и списков для хранения данных
    dict_1, dict_2, dict_3 = {}, {}, {}
    new_etalon_weights = {}
    sign_dict: dict[str, ReferenceSample] = {}

    # Обработка текстовых данных
    # Получаем отдельные тексты из набора текстов
    for text_sample in texts_data:
        # Делим текст на фрагменты
        fragments = split_text_into_fragments(text_sample.text, max_series)
        # Проходимся по фрагментам
        for i, fragment in enumerate(fragments):
            # Составляем внутренний id в таком виде: uuid_part
            fragment_id = "_".join([str(text_sample.id), str(i)])
            # Создаем объект новой обработанной части текста
            new_reference_sample = ReferenceSample(
                id=text_sample.id, part=i, order1=[], order2=[], order3=[], weight=0
            )
            # Заполняем его признаки
            (
                new_reference_sample.order1,
                new_reference_sample.order2,
                new_reference_sample.order3,
            ) = generate_signatures(fragment)
            # Сохраняем его в словаре обработанных частей текстов по fragment_id
            sign_dict[fragment_id] = new_reference_sample
            # Если текст является эталонным, то сохраняем его в отдельном словаре вида uuid_part: label и пропускаем
            if text_sample.label != "?":
                logging.info(f"Got etalon: {int(text_sample.label)}")
                new_etalon_weights[fragment_id] = int(text_sample.label)
                continue
            #
            for etalon in etalons_data:
                # Проверка, не является ли текущий текст эталоном < БОЛЬШОЙ ВОПРОС

                # Обработка фрагментов текущего текста
                if etalon.id == text_sample.id:
                    break
                # Изменение словарей в 3 разных потоках
                with ProcessPoolExecutor(max_workers=3) as executor:
                    dict_1 = executor.submit(
                        update_dictionary,
                        new_reference_sample.order1,
                        etalon.order1,
                        fragment_id,
                        dict_1,
                    ).result()
                    dict_2 = executor.submit(
                        update_dictionary,
                        new_reference_sample.order2,
                        etalon.order2,
                        fragment_id,
                        dict_2,
                    ).result()
                    dict_3 = executor.submit(
                        update_dictionary,
                        new_reference_sample.order3,
                        etalon.order3,
                        fragment_id,
                        dict_3,
                    ).result()

    # Инициализация словаря весов для фрагментов текста
    target_fragments: list[ReferenceSample] = []

    # Расчет весов для фрагментов текста
    for fragment_id in sign_dict.keys():
        # Если фрагмент является эталонным, то берем его эталонное значение
        if fragment_id in new_etalon_weights.keys():
            sign_dict[fragment_id].weight = new_etalon_weights[fragment_id]
        # Иначе расчитываем по весам, полученным при сравнении с другими эталонными значениями
        else:
            sign_dict[fragment_id].weight = (
                3 * dict_1[fragment_id]
                + 2 * dict_2[fragment_id]
                + dict_3[fragment_id]
            ) / 6

        # Проверка, является ли фрагмент текста целевым
        if sign_dict[fragment_id].weight > similarity_border:
            target_fragments.append(sign_dict[fragment_id])
    
    logging.info(sign_dict)

    # Обновление данных базы данных
    etalons_data.extend(sign_dict.values())

    # Запись обновленных данных в файл базы данных
    db.insert_new_samples(etalons_data)

    # Формирование результата - целевых фрагментов и содержащих их текстов

    # Получение id текстов, содержащих целевые фрагменты
    target_text_ids = set([fragment.id for fragment in target_fragments])
    # Получение текстов в виде словаря вида - uuid: текст
    result_target_texts = dict(
        [(text.id, text.text) for text in texts_data if text.id in target_text_ids]
    )
    result_target_fragments = dict(
        [
            (fragment.id, [fragment.part, fragment.weight])
            for fragment in target_fragments
        ]
    )

    return result_target_texts, result_target_fragments


if __name__ == "__main__":
    logging.getLogger("pika").propagate = False
    logging.getLogger('pymorphy2').propagate = False
    # Загрузить переменные окружения из файла .env
    load_dotenv()

    # Получить значения переменных окружения
    db_user = val if (val := os.getenv("DB_USER")) is not None else "postgres"
    db_password = val if (val := os.getenv("DB_PASSWORD")) is not None else "password"
    db_name = val if (val := os.getenv("DB_NAME")) is not None else "postgres"
    db_host = val if (val := os.getenv("DB_HOST")) is not None else "postgres"
    db_port = int(val) if (val := os.getenv("DB_PORT")) is not None else 5432
    rabbit_host = val if (val := os.getenv("MQ_HOST_NAME")) is not None else "I dunno"
    similarity_border = (
        float(val) if (val := os.getenv("SIMILARITY_BORDER")) is not None else 0.7
    )
    # Настройка логера
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    db = Database(db_user, db_password, db_name, db_host, db_port)
    # db.load_json_data("db.json")

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbit_host, heartbeat=900)
    )
    channel = connection.channel()
    queue = channel.queue_declare("texts_analysis")
    queue_name = queue.method.queue

    def callback(ch, method, properties, body):
        payload = json.loads(body.decode())

        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, encoding="utf-8"
        ) as temp_file:
            json.dump(payload, temp_file, ensure_ascii=False)
            temp_file_name = temp_file.name

        # Прямо передаем строку JSON в функцию main_check
        _, target_fragments = main_check(temp_file_name, db, similarity_border)
        # Логирование результата обработки
        logger.info(target_fragments)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(on_message_callback=callback, queue=queue_name)
    channel.start_consuming()
