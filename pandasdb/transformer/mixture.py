class MixtureStep:
    def __init__(self, parent):
        self.__parent__ = parent


class Mixture:
    def __init__(self):
        self.current_step = None

    def __enter__(self):
        self.current_step = MixtureStep(self)
        return self.current_step

    def __exit__(self, exc_type, exc_val, exc_tb):
        for var in vars(self.current_step):
            if not var.startswith("_") and not var.endswith("_"):
                print(var)

    def transform(*args, **kwargs):
        raise NotImplementedError("blah blah")

    def step(self):
        return self
