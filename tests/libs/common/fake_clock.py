class FakeClock:
    """A stand-in for the `time` module that only moves forward when something sleeps

    Patch it over the `time` attribute of the module under test. For async code, also patch `asyncio.sleep` with
    `async_sleep()`. The clock moves only by the sleeps, so use durations that are exact in binary (0.5 or 1.0, not
    0.15). Otherwise a rounding error can make a wait that computes its sleep spin until `max_reads` is hit.

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

    async def async_sleep(self, seconds: float) -> None:
        """Move the clock forward like `sleep()` without yielding to the event loop

        :param seconds: The duration to sleep
        """
        self.sleep(seconds)
