from asyncio import Lock, Semaphore


class SemaphoreControl:

    def __init__(self, settings, logger):
        self._settings = settings
        self._logger = logger
        self.active_semaphores = 0

        self.semaphore = self.initialize_semaphores()

    @property
    def concurrent_limit(self) -> int:
        return self._settings.concurrent_limit

    def initialize_semaphores(self) -> Semaphore:
        self._logger.debug(f"Setting download concurrency to {self.concurrent_limit} threads")
        return Semaphore(self.concurrent_limit)

    async def acquire(self):
        async with Lock():
            await self.semaphore.acquire()
            self.active_semaphores += 1
            self._logger.debug(f"Semaphore acquired - active {self.active_semaphores}")

    async def release(self):
        async with Lock():
            self.semaphore.release()
            self.active_semaphores -= 1
            self._logger.debug(f"Semaphore released - active {self.active_semaphores}")