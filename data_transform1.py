from mrjob.job import MRJob

class BooksDataTransformationData1(MRJob):
    def mapper(self, _, line):
        # Разделение строки на части и обработка данных
        fields = line.split(',')
        if len(fields) >= 7:
            result = (fields[1], fields[6], fields[0], fields[4])
            # Возвращаем данные как строку
            yield "book", str(result)
        else:
            # Вывод ошибочных строк или пропуск
            pass

if __name__ == '__main__':
    BooksDataTransformationData1.run()