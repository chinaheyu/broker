from typing import Generic, TypeVar, Callable, Optional, TypeAlias, Any, cast
from weakref import ref, WeakMethod
from inspect import ismethod
from threading import Lock, Event
from concurrent.futures import Executor, ThreadPoolExecutor


T = TypeVar('T')
Subscriber: TypeAlias = Callable[[T], None]
SubscriberID: TypeAlias = tuple[int, int] | int


class MessageFuture(Generic[T]):
    def __init__(self) -> None:
        self._result: T
        self._event: Event = Event()
        self._lock: Lock = Lock()
        self._done_callbacks: set[Callable[[T], None]] = set()

    def done(self) -> bool:
        return self._event.is_set()

    def result(self, timeout: Optional[float] = None) -> T:
        if self._event.wait(timeout):
            return cast(T, self._result)
        raise TimeoutError()

    def add_done_callback(self, callback: Callable[[T], None]) -> None:
        with self._lock:
            if self._event.is_set():
                callback(self._result)
            else:
                self._done_callbacks.add(callback)

    def __call__(self, message: T) -> None:
        with self._lock:
            if not self._event.is_set():
                self._result = message
                self._event.set()
                for callback in self._done_callbacks:
                    callback(self._result)


class Topic(Generic[T]):
    _executor: Executor = ThreadPoolExecutor()

    def __init__(self, message_type: type[T]) -> None:
        self._message_type: type[T] = message_type
        self._subscribers: dict[SubscriberID, ref[Subscriber[T]]] = {}
        self._subscribers_lock: Lock = Lock()
        self._dead_subscriber: bool = False
        self._latest_message: T | None = None
        self._latest_message_lock: Lock = Lock()

    def __repr__(self) -> str:
        return f'{self.__class__.__qualname__}(message_type={self.message_type.__qualname__})'

    @staticmethod
    def _make_id(target: Subscriber[T]) -> SubscriberID:
        if ismethod(target):
            return id(getattr(target, '__self__')), id(getattr(target, '__func__'))
        return id(target)

    @property
    def message_type(self) -> type[T]:
        return self._message_type

    @property
    def has_subscriber(self) -> bool:
        with self._subscribers_lock:
            self._clean_dead_subscribers()
            return len(self._subscribers) > 0

    def add_subscriber(self, subscriber: Subscriber[T]) -> None:
        if ismethod(subscriber):
            subscriber_ref = cast(ref[Subscriber[T]], WeakMethod(subscriber, self._remove_subscriber))
        elif callable(subscriber):
            if hasattr(subscriber, '__name__') and getattr(subscriber, '__name__') == "<lambda>":
                raise TypeError(f"Lambda expression is unsupported")
            subscriber_ref = ref(subscriber, self._remove_subscriber)
        else:
            raise TypeError(f"Expected a callable type, instead got an unsupported type: {type(subscriber)}")
        with self._subscribers_lock:
            self._clean_dead_subscribers()
            self._subscribers[self._make_id(subscriber)] = subscriber_ref

    def remove_subscriber(self, subscriber: Optional[Subscriber[T]] = None) -> None:
        with self._subscribers_lock:
            self._clean_dead_subscribers()
            if subscriber is None:
                self._subscribers.clear()
            else:
                subscriber_id = self._make_id(subscriber)
                if subscriber_id in self._subscribers:
                    del self._subscribers[subscriber_id]

    def receive(self) -> MessageFuture[T]:
        future = MessageFuture[T]()
        future.add_done_callback(lambda _: self.remove_subscriber(future))
        self.add_subscriber(future)
        return future

    @property
    def latest_message(self) -> MessageFuture[T]:
        with self._latest_message_lock:
            if self._latest_message is None:
                return self.receive()
            else:
                future = MessageFuture[T]()
                future(self._latest_message)
                return future

    def publish(self, message: T) -> None:
        if not isinstance(message, self.message_type):
            raise TypeError(f'Message type should be {self.message_type.__qualname__}, instead of {type(message).__qualname__}')
        with self._latest_message_lock:
            self._latest_message = message
        self._dispatch_message(message)

    def _dispatch_message(self, message: T) -> None:
        with self._subscribers_lock:
            self._clean_dead_subscribers()
            live_subscriber = self._live_subscriber()
        for subscriber in live_subscriber:
            self._executor.submit(subscriber, message)

    def _live_subscriber(self) -> list[Subscriber[T]]:
        all_subscribers = [r() for r in self._subscribers.values()]
        return [r for r in all_subscribers if r is not None]

    def _clean_dead_subscribers(self) -> None:
        if self._dead_subscriber:
            self._dead_subscriber = False
            self._subscribers = {i: r for i, r in self._subscribers.items() if r() is not None}

    def _remove_subscriber(self, _subscriber_ref: Any) -> None:
        self._dead_subscriber = True


def test_topic() -> None:
    my_topic: Topic[str] = Topic(str)
    print(my_topic)

    def subscriber(x: str) -> None:
        print(x)

    my_topic.add_subscriber(subscriber)
    future = my_topic.receive()
    my_topic.latest_message.add_done_callback(lambda x: print(x))

    my_topic.publish('hello world')

    print(future.result())

    del subscriber
    print(f'has_subscriber: {my_topic.has_subscriber}')


if __name__ == '__main__':
    test_topic()
