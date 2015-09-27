import time, fractions, heapq, threading, queue, contextlib
from . import util


class _Context(threading.local):
	def __init__(self):
		super().__init__()
		
		self._current_value = None
	
	@contextlib.contextmanager
	def get_context(self, value):
		assert self._current_value is None
		
		self._current_value = value
		
		try:
			yield
		finally:
			self._current_value = None
	
	@property
	def value(self):
		assert self._current_value is not None
		
		return self._current_value


_current_scheduler = _Context()
_current_post_fn = _Context()


def current_time():
	return _current_scheduler.value.time()


def post(task, *args, **kwargs):
	_current_post_fn.value(lambda: task(*args, **kwargs))


def post_and_wait(task, *args, **kwargs):
	event = threading.Event()
	result = None
	
	def task_fn():
		nonlocal result
		
		result = task(*args, **kwargs)
		event.set()
	
	post(task_fn)
	event.wait()
	
	return result


def schedule(task, delay = 0, *, at = None):
	assert callable(task)
	
	scheduler = _current_scheduler.value
	
	if at is None:
		at = scheduler.time()
	
	scheduler.schedule(task, at + delay)


def interrupt_current_scheduler():
	_current_scheduler.value.interrupt()


def call_with_scheduler(fn):
	"""Runs a function as the first task of a new Scheduler instance.
	
	This function can also be used a decorator."""
	
	scheduler = Scheduler()
	scheduler.post(fn)
	
	scheduler.run()


def with_scheduler(fn):
	def wrapped_fn(*args, **kwargs):
		call_with_scheduler(lambda: fn(*args, **kwargs))
	
	return wrapped_fn


def start_associated_thread(target):
	"""Start a daemon thread which is associated with the current scheduler.
	
	If the target function throws an exception, it will be rethrown on the associated scheduler."""
	
	post_fn = get_post_fn()
	
	def fn():
		with _current_post_fn.get_context(post_fn):
			try:
				target()
			except BaseException as exception:
				def rethrow(exception = exception):
					# This will always run after the thread has been assigned to thread as it is serialized by the scheduler.
					raise Exception('Unhandled exception in thread {}'.format(thread.name)) from exception
				
				post(rethrow)
	
	thread = util.start_daemon_thread(fn)


def get_post_fn():
	return _current_scheduler.value.post


class _Clock:
	"""A filtered clock which provides a monotonically increasing time value which at the same time tries to follow the computer's clock's speed.
	
	time() may return the same value multiple times if asked fast enough. It may jump into the future if the computer's clock does so, but it will never jump into the past."""
	
	def __init__(self):
		if hasattr(time, 'monotonic'):
			time_fn = time.monotonic
		else:
			time_fn = time.time
		
		self._time_fn = time_fn
		self._last_returned_time = 0
		self._current_offset = 0
	
	def time(self):
		res = fractions.Fraction.from_float(self._time_fn()) + self._current_offset
		delta = res - self._last_returned_time
		
		if delta < 0:
			# The clock did walk backwards. We increase the offset by that amount and return the last value of the filtered time value.
			self._current_offset -= delta
			
			return self._last_returned_time
		else:
			# We can return the value as the clock did not walk backwards.
			self._last_returned_time = res
			
			return res


class Scheduler:
	"""A Scheduler is used to run multiple tasks on a single thread.
	
	Tasks are functions that as scheduled using post() or wrap() and may be scheduled to run in the future or as soon as possible. Tasks can be scheduled while the scheduler is running."""
	
	def __init__(self):
		self._clock = _Clock()
		self._heap = [] # List of (time, fn)
		self._sequence = 0
		self._queue = queue.Queue() # Queue of fn.
		self._running = False
		self._current_task_time = None
	
	def schedule(self, task, time):
		assert callable(task)
		
		self._sequence += 1
		heapq.heappush(self._heap, (time, self._sequence, task))
	
	def post(self, task):
		assert callable(task)
		
		self._queue.put(task)
	
	def interrupt(self):
		self._running = False
	
	def time(self):
		return self._current_task_time
	
	def run(self):
		with _current_scheduler.get_context(self):
			try:
				self._running = True
				
				while self._running:
					try:
						if self._heap:
							delay = self._heap[0][0] - self._clock.time()
							task = self._queue.get(delay > 0, delay)
						else:
							task = self._queue.get()
						
						time = self._clock.time()
					except queue.Empty:
						time, _, task = heapq.heappop(self._heap)
					
					self._current_task_time = time
					
					try:
						task()
					finally:
						self._current_task_time = None
			finally:
				self._running = False


class CancellableTask:
	def __init__(self, task):
		"""
		:param () -> T task:
		"""
		
		assert callable(task)
		
		self._task = task
	
	def __call__(self):
		if self._task is not None:
			self._task()
	
	def cancel(self):
		assert self._task
		
		self._task = None


class Timer:
	"""Restartable timer."""
	
	def __init__(self, fn):
		"""fn: Function to call on timeout"""
		
		assert callable(fn)
		
		self._fn = fn
		self._scheduled_task = None
	
	def _handle_timeout(self):
		self._fn()
		self._scheduled_task = None
	
	def start(self, *args, **kwargs):
		"""Start the timer. The arguments are the same as for Scheduler.post() minus the fn argument."""
		
		assert self._scheduled_task is None, 'The timer is already running.'
		
		self._scheduled_task = CancellableTask(self._handle_timeout)
		schedule(self._scheduled_task, *args, **kwargs)
	
	def stop(self):
		"""Stop time timer that's running."""
		
		assert self._scheduled_task is not None, 'The timer is not running.'
		
		self._scheduled_task.cancel()
		self._scheduled_task = None
	
	def restart(self, *args, **kwargs):
		"""Restart the timer that's already running. The arguments are the same as for Scheduler.post() minus the fn argument."""
		
		self.stop()
		self.start(*args, **kwargs)
	
	@property
	def running(self):
		"""Return whether the timer currently has been started but neither stopped nor did it fire."""
		
		return self._scheduled_task is not None
