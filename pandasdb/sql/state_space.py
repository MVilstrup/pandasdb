import json
from collections import defaultdict
import bisect
from copy import deepcopy
from datetime import datetime
from itertools import chain

import pandas as pd
import numpy as np
import numbers
import multiprocessing as mp


class EventList:

    def __init__(self, events, index, key):
        self.events = events
        self.index = index
        self.key = key

    def time_between(self, a, b):
        index_a, index_b = None, None
        for index, event in zip(self.index, self.events):
            if event[self.key] == a:
                index_a = index
                continue

            if event[self.key] == b:
                index_b = index

        if index_a and index_b:
            return (index_b - index_a)

    def __contains__(self, item):
        return item in set([e[self.key] for e in self.events])

    def __bool__(self):
        return bool(self.events)

    def __getitem__(self, item):
        result = self.events[item]
        if not (isinstance(result, list) or isinstance(result, slice)):
            return result[self.key]

        event_values = []
        for r in result:
            event_values.append(r[self.key])

        return event_values


class Event:
    def __init__(self, **data):
        self._data = {}
        for key, value in data.items():
            self._add(key, value)

    def _add(self, key, value):
        self._data[key] = value
        setattr(self, key, value)

    def inherit(self, event):
        for key, value in event:
            if key not in self._data:
                self._data[key] = value

    def __getitem__(self, item):
        return self._data.get(item)

    def __setitem__(self, key, value):
        self._add(key, value)

    def to_dict(self):
        return self._data

    def pop(self, key):
        return self._data.pop(key)

    def __iter__(self):
        return iter(self._data.items())

    def __repr__(self):
        return f"{dict(self)}"


class State:
    def __init__(self, time_key):
        self.time_key = time_key
        self._index = []
        self.events = []
        self.keys = set()

    @property
    def start(self):
        return self._index[0]

    @property
    def end(self):
        return self._index[-1]

    def bisect(self, time):
        return bisect.bisect(self._index, time)

    def add(self, event: dict):
        assert isinstance(event, dict), "Event has to be a dictionary"
        assert self.time_key in event, "Event has to include time key"

        self.keys = self.keys.union(event.keys())

        time = event[self.time_key]
        index = bisect.bisect(self._index, time)

        event = Event(**event)
        if index > 0:
            event.inherit(self.events[index - 1])

        self._index.insert(index, time)
        self.events.insert(index, event)
        return self

    def find_ind_index(self, ts):
        ts = np.array(ts)
        max_index = len(self._index) - 1
        return np.min([np.where(ts < idx, i, max_index) for i, idx in enumerate(self._index)], axis=0)

    def derive_at_times(self, ts, on_column):
        states = []
        for index in self.find_ind_index(ts):
            if index == 0:
                states.append(None)
            else:
                states.append(self.events[index - 1][on_column])

        return states

    def empty(self):
        return Event(**{key: None for key in self.keys})

    def __getitem__(self, item):
        if isinstance(item, str):
            return EventList(self.events, self._index, item)
        elif isinstance(item, int) or isinstance(item, slice):
            return self.events[item]
        elif isinstance(item, datetime):
            idx = self.bisect(item)
            if idx > 0:
                return self.events[idx - 1]
            else:
                return self.empty()

    def __iter__(self):
        return iter(list(zip(self._index, self.events)))

    def __len__(self):
        return len(self.events)


class StateSpace:
    def __init__(self, primary_key, time_key, initial_state={}):
        self.primary_key = primary_key
        self.time_key = time_key
        self._states = defaultdict(lambda: State(time_key))
        self._initial_state = initial_state
        self._keys = set()

    @property
    def start(self):
        return min([state.start for state in self._states.values()])

    @property
    def end(self):
        return max([state.end for state in self._states.values()])

    def initialize(self, key, date):
        assert self._initial_state, "In order to initilialize, the initial state has to be provided"
        # Simple function to showcase the initial state has been reached
        event = deepcopy(self._initial_state)
        event[self.time_key] = date
        self._states[key] = State(self.time_key)
        self._states[key].add(event)

    def add(self, event: dict):
        assert isinstance(event, dict), "Event has to be a dictionary"
        assert self.primary_key in event, "Event has to include primary key"
        assert self.time_key in event, "Event has to include time key"

        self._keys = self._keys.union(event.keys())

        key = event.pop(self.primary_key)
        self._states[key].add(event)

    def derive(self, name, function):
        initial_state = self._empty_event()
        initial_state[name] = None

        for key, state in self._states.items():
            padded_state = [(None, initial_state)] + list(state)
            for (prev_date, prev), (current_date, current) in zip(padded_state, padded_state[1:]):
                current[name] = function(prev, current)

        return self

    def transform(self, function):
        transformed_states = {}
        for key, state_container in self._states.items():
            transformed_states[key] = function(state_container.copy())

        self._states = transformed_states
        return self

    def __getitem__(self, item):
        return self._states[item]

    def __len__(self):
        return sum(map(len, self._states.values()))

    def _empty_event(self):
        return Event(**{key: None for key in self._keys})

    def df(self):
        index, all_events = [], []

        for key, state in self._states.items():
            for date, event in state:
                index.append(date)
                event[self.primary_key] = key
                all_events.append(event.to_dict())

        df = pd.DataFrame(all_events, index=index)
        df = df.sort_index()
        return df

    def infer(self, on, freq="1d", drop_na=True):
        date_range = pd.date_range(self.start, self.end, freq=freq)
        all_changes = [defaultdict(int) for _ in date_range]
        for state in self._states.values():
            for i, res in enumerate(state.derive_at_times(date_range, on_column=on)):
                if res is not None or not drop_na:
                    all_changes[i][res] += 1

        return pd.DataFrame(all_changes, index=date_range)
