from typing import Any
from IPython.display import display, HTML


class Configurable:
    ensured_padding = False

    def __init__(self, name, required=True, default_value: Any = None):
        self.name = name
        self.default_value = default_value
        self.required = required
        self._value = None
        self._callbacks = []

        if not self.ensured_padding:
            display(HTML('''<style> .widget-label { min-width: 15ex !important; } </style>'''))
            self.ensured_padding = True

    @property
    def value(self):
        return self._value if self._value is not None else self.default_value

    @property
    def widget(self):
        raise NotImplementedError()

    def on_change(self, callback: callable):
        self._callbacks.append(callback)
        return self

    def __on_change__(self, change_value):
        prev_value = self._value
        self._value = change_value

        for callback in self._callbacks:
            callback(prev_value, self._value)

    def copy(self):
        raise NotImplementedError()

    def __represent__(self, indent: int = None, line_width: int = None):
        raise NotImplementedError()

    def __len__(self):
        raise NotImplementedError()

    def __repr__(self):
        return self.__represent__()
