class Representable:

    def head(self):
        raise NotImplementedError()

    def __on_head_failure__(self):
        raise NotImplementedError()

    def _repr_fits_horizontal_(self, *args, **kwargs):
        try:
            return self.head()._repr_fits_horizontal_(*args, **kwargs)
        except:
            self.__on_head_failure__()

    def _repr_fits_vertical_(self, *args, **kwargs):
        try:
            return self.head()._repr_fits_vertical_(*args, **kwargs)
        except:
            self.__on_head_failure__()

    def _repr_html_(self, *args, **kwargs):
        try:
            return self.head()._repr_html_(*args, **kwargs)
        except:
            self.__on_head_failure__()

    def _repr_data_resource_(self, *args, **kwargs):
        try:
            return self.head()._repr_data_resource_(*args, **kwargs)
        except:
            self.__on_head_failure__()
