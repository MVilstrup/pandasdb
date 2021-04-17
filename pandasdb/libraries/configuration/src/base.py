
class BaseConfiguration:

    def port(self, port=None):
        raise NotImplementedError()

    def host(self, host=None):
        raise NotImplementedError()

    @property
    def key(self):
        raise NotImplementedError()

    @property
    def valid(self):
        raise NotImplementedError()

    def restart(self):
        raise NotImplementedError()