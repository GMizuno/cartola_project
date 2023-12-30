from abc import ABC, abstractmethod


class Transformer(ABC):
    @abstractmethod
    def transformation(self):
        pass
