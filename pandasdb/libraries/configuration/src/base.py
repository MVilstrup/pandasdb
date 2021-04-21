
class BaseConfiguration:

    @property
    def key(self):
        raise NotImplementedError()

    @property
    def valid(self):
        raise NotImplementedError()

    def restart(self):
        raise NotImplementedError()