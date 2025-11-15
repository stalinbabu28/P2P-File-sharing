"""Stub metrics collector."""

class Collector:
    def __init__(self):
        self.metrics = {}

    def record(self, name: str, value):
        self.metrics.setdefault(name, []).append(value)

    def get(self, name: str):
        return self.metrics.get(name, [])
