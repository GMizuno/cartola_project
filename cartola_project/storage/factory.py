from cartola_project.storage.storage import GCSStorage, LocalStorage


class StorageFactory:
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


factory = StorageFactory()
factory.register_format('GCP', GCSStorage)
factory.register_format('Local', LocalStorage)
