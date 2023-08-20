from cartola_project.reader.reader import JSONReader, ParquetReader


class ReaderFactory:
    def __init__(self):
        self._creators = {}

    def register_format(self, format, creator):
        self._creators[format] = creator

    def get_storage(self, format):
        creator = self._creators.get(format)
        if not creator:
            raise ValueError(format)
        return creator

    def show_creators(self):
        return self._creators


factory = ReaderFactory()
factory.register_format('JSON', JSONReader)
factory.register_format('Parquet', ParquetReader)
