from typing import Generic, TypeVar, Callable, Optional, Generator, TypeAlias, Any, get_type_hints, get_origin, get_args, cast
from weakref import ref, WeakMethod
from inspect import ismethod
from threading import Lock, Event
from concurrent.futures import Executor, ThreadPoolExecutor


T = TypeVar('T')
SUBSCRIBER: TypeAlias = Callable[[T], None]
SUBSCRIBER_ID: TypeAlias = tuple[int, int] | int


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

    def __call__(self, result: T) -> None:
        with self._lock:
            if not self._event.is_set():
                self._result = result
                self._event.set()
                for callback in self._done_callbacks:
                    callback(self._result)


class Topic(Generic[T]):
    def __init__(self, name: str, message_type: type[T]) -> None:
        self._name: str = name
        self._message_type: type[T] = message_type
        self._subscribers: dict[SUBSCRIBER_ID, ref[SUBSCRIBER[T]]] = {}
        self._subscribers_lock: Lock = Lock()
        self._dead_subscriber: bool = False
        self._latest_message: T | None = None
        self._latest_message_lock: Lock = Lock()

    def __repr__(self) -> str:
        return f'{self.__class__.__qualname__}(name="{self.name}", message_type={self.message_type.__qualname__})'

    @staticmethod
    def _make_id(target: SUBSCRIBER[T]) -> SUBSCRIBER_ID:
        if ismethod(target):
            return id(getattr(target, '__self__')), id(getattr(target, '__func__'))
        return id(target)

    @property
    def name(self) -> str:
        return self._name

    @property
    def message_type(self) -> type[T]:
        return self._message_type

    @property
    def has_subscriber(self) -> bool:
        with self._subscribers_lock:
            self._clean_dead_subscribers()
            return len(self._subscribers) > 0

    def add_subscriber(self, subscriber: SUBSCRIBER[T]) -> None:
        if ismethod(subscriber):
            subscriber_ref = cast(ref[SUBSCRIBER[T]], WeakMethod(subscriber, self._remove_subscriber))
        elif callable(subscriber):
            if hasattr(subscriber, '__name__') and getattr(subscriber, '__name__') == "<lambda>":
                raise TypeError(f"Lambda expression is unsupported")
            subscriber_ref = ref(subscriber, self._remove_subscriber)
        else:
            raise TypeError(f"Expected a callable type, instead got an unsupported type: {type(subscriber)}")
        with self._subscribers_lock:
            self._clean_dead_subscribers()
            self._subscribers[self._make_id(subscriber)] = subscriber_ref

    def remove_subscriber(self, subscriber: Optional[SUBSCRIBER[T]] = None) -> None:
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
            subscriber(message)

    def _live_subscriber(self) -> list[SUBSCRIBER[T]]:
        all_subscribers = [r() for r in self._subscribers.values()]
        return [r for r in all_subscribers if r is not None]

    def _clean_dead_subscribers(self) -> None:
        if self._dead_subscriber:
            self._dead_subscriber = False
            self._subscribers = {i: r for i, r in self._subscribers.items() if r() is not None}

    def _remove_subscriber(self, _subscriber_ref: Any) -> None:
        self._dead_subscriber = True


class Broker:
    def __init__(self, namespace: str = '/') -> None:
        for n, t in get_type_hints(self.__class__).items():
            attribute_type = get_origin(t)
            if attribute_type is None:
                attribute_type = t
            if issubclass(attribute_type, Broker):
                setattr(self, n, t(namespace + n + '/'))
            elif issubclass(attribute_type, Topic):
                topic = Topic(namespace + n, get_args(t)[0])
                setattr(self, n, topic)
            else:
                raise TypeError("Value type should be Topic[T] or BrokerType")

    def __iter__(self) -> Generator[Topic, None, None]:
        for topic_or_broker in self.__dict__.values():
            if isinstance(topic_or_broker, Topic):
                yield topic_or_broker
            if isinstance(topic_or_broker, Broker):
                yield from topic_or_broker

    def __getitem__(self, topic_name: str) -> Topic:
        for topic in self:
            if topic.name == topic_name:
                return topic
        raise KeyError(topic_name)

    def __setitem__(self, topic_name: str, message_type: Any) -> None:
        paths = topic_name.split('/')
        broker = self
        for depth, namespace in enumerate(paths[1:-1]):
            if hasattr(broker, namespace):
                sub_broker = getattr(broker, namespace)
            else:
                sub_broker = Broker('/'.join(paths[:depth + 1]) + '/')
                setattr(broker, namespace, sub_broker)
            broker = sub_broker
        setattr(broker, paths[-1], Topic[message_type](topic_name, message_type))

    def __contains__(self, topic_name: str) -> bool:
        for topic in self:
            if topic.name == topic_name:
                return True
        return False


class Subscriber(Generic[T]):
    def __init__(self, topic: Topic[T], callback: Callable[[T], None], executor: Executor) -> None:
        self._topic = topic
        self._callback = callback
        self._executor = executor
        self.subscribe()

    def __call__(self, message: T) -> None:
        self._executor.submit(self._callback, message)

    def subscribe(self) -> None:
        self._topic.add_subscriber(self)

    def unsubscribe(self) -> None:
        self._topic.remove_subscriber(self)


class Node:
    broker: Broker = Broker()
    executor: Executor = ThreadPoolExecutor()

    @classmethod
    def _ensure_topic(cls, topic_name: str, message_type: type[T]) -> Topic:
        if topic_name not in cls.broker:
            cls.broker[topic_name] = message_type
        topic = cls.broker[topic_name]
        if topic.message_type is not message_type:
            raise TypeError(f'Message type of {topic_name} should be {message_type.__qualname__}')
        return topic

    @classmethod
    def subscribe(cls, topic_name: str, message_type: type[T], callback: Callable[[T], None]) -> Subscriber[T]:
        topic = cls._ensure_topic(topic_name, message_type)
        return Subscriber(topic, callback, cls.executor)

    @classmethod
    def publish(cls, topic_name: str, message_type: type[T], message: T) -> None:
        topic = cls._ensure_topic(topic_name, message_type)
        topic.publish(message)


def test_topic() -> None:
    topic = Topic[str]('my_topic', str)
    print(topic)

    def subscriber(x: str) -> None:
        print(x)

    topic.add_subscriber(subscriber)
    topic.latest_message.add_done_callback(lambda x: print(f'Done: {x}'))

    topic.publish('hello world')

    del subscriber
    print(f'has_subscriber: {topic.has_subscriber}')


def test_broker() -> None:
    class MySubBroker(Broker):
        topic1: Topic[str]

    class MyBroker(Broker):
        sub_broker: MySubBroker
        topic2: Topic[str]

    my_broker = MyBroker()
    my_broker['/sub_broker/topic3'] = str
    my_broker['/sub_broker/sub_broker/topic4'] = str

    print([t for t in my_broker])

    def my_subscriber(message: str) -> None:
        print(message)

    my_broker.sub_broker.topic1.add_subscriber(my_subscriber)
    my_broker.topic2.add_subscriber(my_subscriber)
    my_broker['/sub_broker/topic3'].add_subscriber(my_subscriber)
    my_broker['/sub_broker/sub_broker/topic4'].add_subscriber(my_subscriber)

    my_broker.sub_broker.topic1.publish("hello world 1")
    my_broker.topic2.publish("hello world 2")
    my_broker['/sub_broker/topic3'].publish("hello world 3")
    my_broker['/sub_broker/sub_broker/topic4'].publish("hello world 4")

    del my_subscriber

    print(f'has_subscriber: {any((i.has_subscriber for i in my_broker))}')


def test_node() -> None:
    my_node = Node()
    subscriber1 = my_node.subscribe('/topic1', str, lambda x: my_node.publish('/topic2', str, x))
    subscriber2 = my_node.subscribe('/topic2', str, lambda x: print(x))
    my_node.publish('/topic1', str, 'hello world')
    del subscriber1
    subscriber2.unsubscribe()
    print(f'has_subscriber: {any((i.has_subscriber for i in my_node.broker))}')


if __name__ == '__main__':
    test_topic()
    test_broker()
    test_node()
