from cartola_project.writer.writer import JSONWriter, ParquetWriter


class WriterFactory:
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


factory = WriterFactory()
factory.register_format('JSON', JSONWriter)
factory.register_format('Parquet', ParquetWriter)
