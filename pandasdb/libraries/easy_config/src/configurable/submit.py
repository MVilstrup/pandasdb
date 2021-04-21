from ipywidgets import widgets

from pandasdb.libraries.easy_config.src.configurable import Configurable
from pandasdb.libraries.easy_config.src.configurable.validator import Validator


class Submit(Configurable):
    def __init__(self, config: Configurable):
        super().__init__(f"{config.name} Validation")
        self.config = config

    @property
    def widget(self):
        validation = widgets.Valid(value=False, description=f'{self.config.name} Config')
        return widgets.VBox([self.config.widget, validation])


class ValidatedSubmit(Configurable):
    def __init__(self, on_submit: callable, on_cancel: callable, validator: Validator):
        super().__init__(f"{validator.name} Validation")
        self.validator = validator

    @property
    def widget(self):
        validation = widgets.Valid(value=False, description=f'{self.validator.name} Config')
        return widgets.VBox([self.validator.widget, validation])
