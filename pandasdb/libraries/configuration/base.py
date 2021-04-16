
class BaseConfiguration:

    @property
    def key(self):
        raise NotImplementedError()

    def restart(self):
        raise NotImplementedError()