from ipywidgets import widgets

from pandasdb.libraries.easy_config.src.configurable import Configurable


class Validator(Configurable):
    def __init__(self, validation_callback: callable, config: Configurable):
        super().__init__(f"{config.name} Validation")
        self.config = config

    @property
    def widget(self):
        validation = widgets.Valid(value=False, description=f'{self.config.name} Config')
        return widgets.VBox([self.config.widget, validation])