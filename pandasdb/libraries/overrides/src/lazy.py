class LazyLoader:
    def __init__(self, should_setup=True):
        self.__is_setting_up__ = False
        self.__has_setup__ = not should_setup

    def __do_setup__(self):
        if not self.__has_setup__:
            self.__is_setting_up__ = True
            self.__setup__()
            self.__is_setting_up__ = False
            self.__has_setup__ = True

    def __setup__(self, timeout=10):
        raise NotImplementedError()

    def refresh(self, timeout=10):
        self.__setup__(timeout)
        return self

    def __getattr__(self, item):
        if item in ["__setup__", "__has_setup__", "__is_setting_up__", "__do_setup__"]:
            return self.__getattribute__(item)

        if self.__is_setting_up__:
            return self.__getattribute__(item)
        else:
            self.__do_setup__()

        return self.__getattribute__(item)

    def __dir__(self):
        try:
            self.__do_setup__()
        except:
            pass

        return super().__dir__()
