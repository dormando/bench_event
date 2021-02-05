Completely ignorable exploration codebase.

This is a benchmark for various approaches to having shared proxy backend
connections in a multi-threaded memcached proxy. I've been experimenting
with various libevent approaches to see how threads scale and what the perf
output looks like. It's pretty bleak.

I've also been learning `io_uring` and figuing out how to plug it in here.
It's not as straightforward as I hoped:

- In this proxy case we want to stuff requests sequentially down a backend, note
  them in a queue, and read them back at any point.
- Due to async nature of `io_uring`, this means we can get a read before a
  write is confirmed. Or writes are done out of order, etc.
- Which means we have to confirm writes and then schedule reads, vs simply
  waiting on a persistent epoll read hook. In the libevent code this is
mitigated by holding a lock around the backend object during the write
syscall.

I've also tried handing writes off to the event threads but this scales
poorly. My next attempt at `io_uring` will be to batch the write syscall
across different backends simultaneously.

Mixing uring and libevent isn't always easy either. Also the timers aren't
super fast.
