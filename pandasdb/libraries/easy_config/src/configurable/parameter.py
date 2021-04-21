from pandasdb.libraries.easy_config.src.configurable import Configurable
from typing import List, Any
import ipywidgets as widgets

class Parameter(Configurable):
    def __init__(self, name: str, type: str, possible_values: List[str] = None, default_value: Any = None,
                 required=True, *args):
        super().__init__(name, required, default_value)
        self.type = type
        self.possible_values = possible_values
        self.args = args

    @property
    def widget(self):
        def setup(widget):
            widget.observe(lambda change: self.__on_change__(change.new), 'value')
            return widget

        if self.possible_values:
            if "multi" in self.args:
                widget = widgets.SelectMultiple(value=self.value,
                                                options=self.possible_values,
                                                description=f'{self.name.capitalize()}')
            else:
                widget = widgets.Dropdown(value=self.value,
                                          options=self.possible_values,
                                          description=f'{self.name.capitalize()}')
            return setup(widget)
        else:
            available = {
                "string": widgets.Text,
                "integer": widgets.IntText,
                "float": widgets.FloatText,
                "boolean": widgets.Checkbox,
                "password": widgets.Password,
            }

            widget_cls = available.get(self.type)
            if not widget_cls:
                raise ValueError(f"'{self.type}' not among the valid types: {list(available.keys())}")
            else:
                widget = widget_cls(value=self.value,
                                    description=f'{self.name.capitalize()}',
                                    # style=dict(description_width='initial'),
                                    layout=dict(width='400px'))
                return setup(widget)

    def __represent__(self, indent: int = None, line_width: int = None):
        indendation = ' ' * indent if indent else ''
        padding = ' ' * (line_width - len(self)) if line_width else ''
        return f"{indendation}{self.name}:  {padding}{self.value}"

    def __len__(self):
        return len(self.name) + len(str(self.value)) + 2
