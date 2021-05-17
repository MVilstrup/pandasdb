import pandas as pd


class JoinOperator:
    def __init__(self, *on, lsuffix="_left", rsuffix="_right"):
        if len(on) == 2:
            self.on_left, self.on_right = on
        elif len(on) == 1:
            on = on[0]
            self.on_left, self.on_right = on, on
        else:
            raise ValueError(f"{on} is not valid column names for a join. Please specify only on or two columns")

        self.lsuffix = lsuffix
        self.rsuffix = rsuffix
        self.inputs = {}

    def __validate__(self, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            if not hasattr(df, "df") and callable(getattr(df, "df")):
                raise ValueError("Join only works on pandas dataframes or objects with '.df()' function")
            else:
                return self.__validate__(df.df())

        assert isinstance(df, pd.DataFrame), "Join only works on pandas dataframes"

        return self.__clean__(df)

    def __clean__(self, df: pd.DataFrame):
        if not isinstance(df.index, (pd.Int64Index, pd.RangeIndex)):
            df = df.reset_index()

        return df

    def __lshift__(self, df):
        self.inputs["lshift"] = self.__validate__(df)

        if len(self.inputs) == 2:
            return self.__compute__()
        else:
            return self

    def __rshift__(self, df):
        self.inputs["rshift"] = self.__validate__(df)

        if len(self.inputs) == 2:
            return self.__compute__()
        else:
            return self

    def __rlshift__(self, df):
        self.inputs["rlshift"] = self.__validate__(df)

        if len(self.inputs) == 2:
            return self.__compute__()
        else:
            return self

    def __rrshift__(self, df):
        self.inputs["rrshift"] = self.__validate__(df)

        if len(self.inputs) == 2:
            return self.__compute__()
        else:
            return self

    def __compute__(self):
        def join(left, right, how, index_name):
            left = left.set_index(self.on_left)
            right = right.set_index(self.on_right)

            joined = left.join(right, how=how, lsuffix=self.lsuffix, rsuffix=self.rsuffix)
            joined.index.name = index_name
            return joined

        keys = set(self.inputs.keys())
        if keys == {"rlshift", "lshift"}:
            # Left Join
            return join(left=self.inputs["rlshift"], right=self.inputs["lshift"], how="left", index_name=self.on_left)
        elif keys == {"rrshift", "rshift"}:
            # Right Join
            return join(left=self.inputs["rrshift"], right=self.inputs["rshift"], how="right", index_name=self.on_right)
        elif keys == {"rrshift", "lshift"}:
            # Inner Join
            return join(left=self.inputs["rrshift"], right=self.inputs["lshift"], how="inner", index_name=self.on_left)
        elif keys == {"rlshift", "rshift"}:
            # Outer Join
            return join(left=self.inputs["rlshift"], right=self.inputs["rshift"], how="outer", index_name=self.on_left)


class SimpleJoinOperator:
    def __init__(self, on, how, lsuffix="_left", rsuffix="_right"):
        if len(on) == 2:
            self.on_left, self.on_right = on
        elif len(on) == 1:
            on = on[0]
            self.on_left, self.on_right = on, on
        else:
            raise ValueError(f"{on} is not valid column names for a join. Please specify only on or two columns")

        self.lsuffix = lsuffix
        self.rsuffix = rsuffix
        self.how = how
        self.inputs = {}

    def __validate__(self, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            if not hasattr(df, "df") and callable(getattr(df, "df")):
                raise ValueError("Join only works on pandas dataframes or objects with '.df()' function")
            else:
                return self.__validate__(df.df())

        assert isinstance(df, pd.DataFrame), "Join only works on pandas dataframes"

        return self.__clean__(df)

    def __clean__(self, df: pd.DataFrame):
        if not isinstance(df.index, (pd.Int64Index, pd.RangeIndex)):
            df = df.reset_index()

        return df

    def __lshift__(self, df):
        self.inputs["lshift"] = self.__validate__(df)

        if len(self.inputs) == 2:
            return self.__compute__()
        else:
            return self

    def __rrshift__(self, df):
        self.inputs["rrshift"] = self.__validate__(df)

        if len(self.inputs) == 2:
            return self.__compute__()
        else:
            return self

    def __compute__(self):
        left = self.inputs["rrshift"].set_index(self.on_left)
        right = self.inputs["lshift"].set_index(self.on_right)

        joined = left.join(right, how=self.how, lsuffix=self.lsuffix, rsuffix=self.rsuffix)

        if self.how == "right":
            index_name = self.on_right
        else:
            index_name = self.on_left

        joined.index.name = index_name
        return joined


def On(*on, lsuffix=None, rsuffix="_right"):
    return JoinOperator(*on, lsuffix=lsuffix, rsuffix=rsuffix)


def Left(*on, lsuffix=None, rsuffix="_right"):
    return SimpleJoinOperator(on, how="left", lsuffix=lsuffix, rsuffix=rsuffix)


def Right(*on, lsuffix=None, rsuffix="_right"):
    return SimpleJoinOperator(on, how="right", lsuffix=lsuffix, rsuffix=rsuffix)


def Inner(*on, lsuffix=None, rsuffix="_right"):
    return SimpleJoinOperator(on, how="inner", lsuffix=lsuffix, rsuffix=rsuffix)


def Outer(*on, lsuffix=None, rsuffix="_right"):
    return SimpleJoinOperator(on, how="outer", lsuffix=lsuffix, rsuffix=rsuffix)
