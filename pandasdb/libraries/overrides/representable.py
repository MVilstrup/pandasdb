
class Representable:

    def head(self):
        raise NotImplementedError()

    def _repr_fits_horizontal_(self, *args, **kwargs):
        return self.head()._repr_fits_horizontal_(*args, **kwargs)

    def _repr_fits_vertical_(self, *args, **kwargs):
        return self.head()._repr_fits_vertical_(*args, **kwargs)

    def _repr_html_(self, *args, **kwargs):
        return self.head()._repr_html_(*args, **kwargs)

    def _repr_latex_(self, *args, **kwargs):
        return self.head()._repr_latex_(*args, **kwargs)

    def _repr_data_resource_(self, *args, **kwargs):
        return self.head()._repr_data_resource_(*args, **kwargs)