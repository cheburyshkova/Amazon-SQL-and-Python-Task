from mrjob.job import MRJob

class BooksDataTransformationData2(MRJob):

    def mapper(self, _, line):
        # Пропуск заголовка
        if line.startswith('id'):
            return
        # Разделение строки на части
        fields = line.split(',')
        # Вывод необходимых полей: Название, Жанр(ы), Автор, Год
        # При необходимости можно добавить логику для обработки множественных жанров
        yield "book", (fields[1], fields[2], fields[3], fields[4])

    def reducer(self, key, values):
        for value in values:
            yield key, value

if __name__ == '__main__':
    BooksDataTransformationData2.run()