class FakeClock:
    """A stand-in for the `time` module that only moves forward when something sleeps

    :param max_reads: The number of readings after which the clock raises, so that a wait that never ends fails instead
                      of hanging
    """

    def __init__(self, max_reads: int = 1000) -> None:
        self.sleeps: list[float] = []
        self._now = 0.0
        self._reads = 0
        self._max_reads = max_reads

    def monotonic(self) -> float:
        """Return the current time"""
        self._reads += 1
        if self._reads > self._max_reads:
            raise AssertionError(f"The clock was read more than {self._max_reads} times. The wait does not end")
        return self._now

    def sleep(self, seconds: float) -> None:
        """Move the clock forward by the given seconds and remember the sleep

        :param seconds: The duration to sleep
        """
        self.sleeps.append(seconds)
        self._now += seconds
