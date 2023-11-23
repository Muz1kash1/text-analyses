# Импорт необходимых библиотек и модулей
import sys
import json
import pymorphy2
import re
from nltk import sent_tokenize, word_tokenize, pos_tag
from database import DatabaseLegacy


def pymorphy2_311_hotfix():
    from inspect import getfullargspec
    from pymorphy2.units.base import BaseAnalyzerUnit

    def _get_param_names_311(klass):
        if klass.__init__ is object.__init__:
            return []
        args = getfullargspec(klass.__init__).args
        return sorted(args[1:])

    setattr(BaseAnalyzerUnit, '_get_param_names', _get_param_names_311)


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
    morph = pymorphy2.MorphAnalyzer()

    # Получение разбора слова и выбор первого варианта (наиболее вероятного)
    parsed_word = morph.parse(word)[0]

    # Возвращение нормальной формы слова
    return parsed_word[2]


def split_text_into_fragments(text: str, max_series=5) -> list:
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
        fragments.append(sentences[index:index + max_series])
        series = (len(sentences) - len(fragments) * max_series) // max_series

    # Обработка оставшихся предложений после создания максимального числа фрагментов
    last_index = len(sentences) // max_series
    if len(sentences) % max_series:
        fragments.append(sentences[last_index * max_series:])

    # Возвращение списка фрагментов
    return fragments


def extract_first_signs(fragment: list) -> list:
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
        cleaned_word_list = [re.sub('[^а-яё]', '', word.lower()) for word in word_list if len(word) > 2]

        # Удаление пустых строк из списка слов
        cleaned_word_list = list(filter(('').__ne__, cleaned_word_list))

        # Нормализация слов и добавление их в список признаков
        signs.append(list(set(map(normalize_word, cleaned_word_list))))

    # Возвращение списка признаков первого уровня
    return signs


def extract_second_signs(signs_list: list) -> list:
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
        parts_of_speech = pos_tag(sign_set, lang='rus')
        noun_verb_signs = [part[0] for part in parts_of_speech if part[1][0] in ['S', 'V']]
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


def extract_third_signs(signs_list: list) -> list:
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
        parts_of_speech = pos_tag(sign_set, lang='rus')
        noun_signs = [part[0] for part in parts_of_speech if part[1][0] == 'S']
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
    if length % 3:
        signs.append(get_nouns(signs_list[-2] + signs_list[-1]))

    # Возвращение списка признаков третьего уровня
    return signs


def generate_signatures(fragment: list) -> list:
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


def compare_signatures(signature1: list, signature2: list) -> list:
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


def get_text_id(fragment_id: str, text_id_length=6) -> str:
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


def update_dictionary(sign_list: list, etalon_fragment: list, fragment_id: str, dictionary: dict) -> dict:
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
    weights_list = [sorted([compare_signatures(sign, etalon_sign) for etalon_sign in etalon_fragment], key=lambda x: x[0] / x[1])[-1] for sign in sign_list]

    # Обновление словаря весов для данного фрагмента текста
    dictionary[fragment_id] = sorted(weights_list, key=lambda x: x[0] / x[1])[-1]

    # Возвращение обновленного словаря
    return dictionary


def main_check(input_filename: str, db: DatabaseLegacy, similarity_border=0.1, max_series=5, id_legend=[6, 3]):
    """
    Основная функция для проверки схожести фрагментов текста с эталонами и обновления базы данных.

    Параметры:
    - input_filename (str): Имя файла с входными данными в формате JSON.
    - db_filename (str): Имя файла базы данных с эталонами в формате JSON.
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
    texts_data = {}
    with open(input_filename, "r", encoding='utf-8') as read_file:
        json_data = json.load(read_file)
        for item in json_data:
            texts_data[item['id']] = [item['text'], item['label']]

    # Чтение данных эталонов из Postgres
    etalons_data = db.get_reference_samples()
    # etalons_data = {}
    # with open(db_filename, "r", encoding='utf-8') as read_file:
    #     json_data = json.load(read_file)
    #     for item in json_data:
    #         order_1 = [part.split(',') for part in item['order1'].split(';')]
    #         order_2 = [part.split(',') for part in item['order2'].split(';')]
    #         order_3 = [part.split(',') for part in item['order3'].split(';')]
    #         if all([order_1, order_2, order_3]):
    #             etalons_data[item['id']] = [order_1, order_2, order_3, item['weight']]

    # Инициализация словарей и списков для хранения данных
    dict_1, dict_2, dict_3 = {}, {}, {}
    new_etalon_weights, sign_dict = {}, {}

    # Обработка текстовых данных
    for text_id in texts_data.keys():
        fragments = split_text_into_fragments(texts_data[text_id][0], max_series)

        for etalon_id in etalons_data.keys():
            # Проверка, не является ли текущий текст эталоном
            if texts_data[text_id][1] != '?':
                new_etalon_weights[etalon_id] = int(texts_data[text_id][1])

            # Обработка фрагментов текущего текста
            if get_text_id(etalon_id, id_legend[0]) != text_id:
                for i, fragment in enumerate(fragments):
                    fragment_id = text_id + '0' * (id_legend[1] - len(str(i))) + str(i)
                    sign_dict[fragment_id] = generate_signatures(fragment)
                    dict_1 = update_dictionary(sign_dict[fragment_id][0], etalons_data[etalon_id][0], fragment_id, dict_1)
                    dict_2 = update_dictionary(sign_dict[fragment_id][1], etalons_data[etalon_id][1], fragment_id, dict_2)
                    dict_3 = update_dictionary(sign_dict[fragment_id][2], etalons_data[etalon_id][2], fragment_id, dict_3)
            else:
                break

    # Инициализация словаря весов для фрагментов текста
    fragment_weights = {}
    target_fragment_ids = []
    new_etalons_data = []

    # Расчет весов для фрагментов текста
    for fragment_id in dict_1.keys():
        if fragment_id in new_etalon_weights:
            fragment_weights[fragment_id] = new_etalon_weights[fragment_id]
        else:
            fragment_weights[fragment_id] = (3 * dict_1[fragment_id][0] / dict_1[fragment_id][1] +
                                             2 * dict_2[fragment_id][0] / dict_2[fragment_id][1] +
                                             dict_3[fragment_id][0] / dict_3[fragment_id][1]) / 6

        # Проверка, является ли фрагмент текста целевым
        if fragment_weights[fragment_id] > similarity_border:
            target_fragment_ids.append(fragment_id)

        # Создание строк для представления признаков в виде текста
        str_sig_1 = ';'.join([','.join(sign_set) for sign_set in sign_dict[fragment_id][0]])
        str_sig_2 = ';'.join([','.join(sign_set) for sign_set in sign_dict[fragment_id][1]])
        str_sig_3 = ';'.join([','.join(sign_set) for sign_set in sign_dict[fragment_id][2]])

        # Создание новых эталонных данных для целевых фрагментов
        if fragment_weights[fragment_id]:
            new_etalons_data.append({'id': fragment_id, 'order1': str_sig_1, 'order2': str_sig_2,
                                     'order3': str_sig_3, 'weight': fragment_weights[fragment_id]})

    # Обновление данных базы данных
    current_data = []
    data = db.get_reference_samples_scuffed()
    for item in data:
        if item["id"] in new_etalon_weights:
            if new_etalon_weights[item["id"]]:
                current_data.append({'id': item['id'], 'order1': item['order1'], 'order2': item['order2'],
                                     'order3': item['order3'], 'weight': new_etalon_weights[item['id']]})
            else:
                current_data.append(item)
    # with open(db_filename, "r", encoding='utf-8') as file:
    #     data = json.load(file)
    #     for item in data:
    #         if item['id'] in new_etalon_weights:
    #             if new_etalon_weights[item['id']]:
    #                 current_data.append({'id': item['id'], 'order1': item['order1'], 'order2': item['order2'],
    #                                      'order3': item['order3'], 'weight': new_etalon_weights[item['id']]})
    #         else:
    #             current_data.append(item)

    # Добавление новых эталонных данных
    for new_etalon in new_etalons_data:
        if new_etalon['id'] not in [existing_data['id'] for existing_data in current_data]:
            current_data.append(new_etalon)

    # Запись обновленных данных в файл базы данных
    db.insert_new_samples(current_data)
    # with open(db_filename, "w", encoding='utf-8') as output_file:
    #     json.dump(current_data, output_file, ensure_ascii=False)

    # Формирование результата - текстов и целевых фрагментов
    target_texts = {}
    target_fragments = {}

    for target_fragment_id in target_fragment_ids:
        target_text_id = get_text_id(target_fragment_id[:id_legend[0]])
        if target_text_id not in target_texts.keys():
            target_texts[target_text_id] = texts_data[target_text_id][0]

        fragments = split_text_into_fragments(target_texts[target_text_id], max_series)
        target_fragments[target_fragment_id] = [fragments[int(target_fragment_id[-id_legend[1]:])],
                                                fragment_weights[target_fragment_id]]

    return target_texts, target_fragments


if __name__ == "__main__":
    pymorphy2_311_hotfix()
    db = DatabaseLegacy("postgres", "password", "postgres", "localhost", 5432)
    db.clear_table
    db.load_json_data("db.json")
    input_file = sys.argv[1]
    similarity_border = float(sys.argv[2])
    _, target_fragments = main_check(input_file, db, similarity_border)
    print(target_fragments)


