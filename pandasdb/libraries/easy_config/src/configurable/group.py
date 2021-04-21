from functools import partial

from ipywidgets import widgets

from pandasdb.libraries.easy_config.src.configurable import Configurable


class Group(Configurable):
    def __init__(self, name, *parameters):
        super().__init__(name=name,
                         default_value={"type": name.upper(), "config": {p.name.upper(): p.value for p in parameters}})
        self.parameters = parameters
        for param in self.parameters:
            param.on_change(partial(self.__hook_changes__, param_name=param.name))

    def __hook_changes__(self, previous_value, current_value, param_name):
        state = self.value
        state["config"][param_name.upper()] = current_value
        self.__on_change__(state)

    @property
    def widget(self):
        to_display = [
            widgets.HTML(value=f"<h3>{self.name.capitalize()}</h3>"),
            *[p.widget for p in self.parameters]
        ]
        return widgets.VBox(to_display)

    def __represent__(self, indent: int = 0, line_width: int = None):
        line_width = max(len(self), line_width) if line_width else len(self)

        parameters = "\n".join([p.__represent__(indent=indent + 2, line_width=line_width) for p in self.parameters])
        return f"{' ' * indent}{self.name}\n{parameters}"

    def __len__(self):
        return max(map(len, self.parameters))
