from pandasdb.libraries.easy_config.src.configurable import Configurable
from pandasdb.libraries.easy_config.src.configurable.parameter import Parameter
import ipywidgets as widgets
from IPython.display import display, clear_output


class Conditional(Configurable):
    def __init__(self, name: str, config: Configurable):
        super().__init__(name)
        self.config = config
        self.check = Parameter(self.name, "boolean")
        self._checked = False

    @property
    def widget(self):
        checked = self.check.widget
        out = widgets.Output()

        def on_change(change):
            self._checked = not self._checked
            with out:

                if self._checked:
                    display(self.config.widget)
                else:
                    clear_output()

        checked.observe(on_change)

        return widgets.VBox([checked, out])
