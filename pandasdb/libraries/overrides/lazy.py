
class LazyLoader:
    def __init__(self):
        self.__has_setup__ = False

    def __setup__(self):
        raise NotImplementedError()

    def __getattr__(self, item):
        if item in ["__setup__", "__has_setup__"]:
            return self.__getattribute__(item)

        if not self.__has_setup__:
            self.__setup__()
            self.__has_setup__ = True

        return self.__getattribute__(item)

    def __dir__(self):
        if not self.__has_setup__:
            self.__setup__()

        return super().__dir__()
