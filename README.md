# async-network

**English** | [简体中文](README.zh-CN.md)

A user-space network stack built on **C++23 coroutines**, a **multi-threaded io_uring scheduler**, and **AF_PACKET capture**.

The library does not go through the kernel TCP/IP stack: it reads raw Ethernet frames from a Linux virtual NIC (`eth0` under WSL2), implements Ethernet / ARP / IPv4 / ICMPv4 / UDP / TCP by itself, and writes frames back through the same NIC. Applications get a `co_await`-style asynchronous socket API, so network code reads like synchronous code.

```cpp
auto listener = co_await TcpListener::bind("192.168.1.100:7891");
auto client = co_await listener.accept();
co_await client.write("hello");
```

## Usage Examples

### UDP echo

```cpp
#include <async/coro.h>
#include <async/socket.h>

co_async<> udp_echo(std::string host)
{
    auto binder = co_await coro::net::UdpSocket::bind(host + ":7890");
    if (not binder)
    {
        co_return;
    }

    auto socket = std::move(binder.value());
    std::vector<uint8_t> storage(4096);
    std::span data(storage);

    while (true)
    {
        auto result = co_await socket.receive_from(data);
        if (not result)
        {
            break;
        }

        auto [count, client] = result.value();
        co_await socket.send_to(data.first(count), client);
    }
}
```

### TCP echo

```cpp
co_async<> tcp_echo(std::string host)
{
    auto binder = co_await coro::net::TcpListener::bind(host + ":7891");
    if (not binder)
    {
        co_return;
    }

    auto listener = std::move(binder.value());
    while (true)
    {
        auto accepted = co_await listener.accept();   // suspends until the handshake completes
        if (not accepted)
        {
            break;
        }

        auto socket = std::move(accepted.value());
        auto [rx, tx] = std::move(socket).split();    // read and write halves run independently
        coro::spawn(echo_loop(std::move(rx), std::move(tx)));
    }
}
```

### Starting the runtime

```cpp
NetConfig config;
std::vector<std::string> errors;
if (not NetConfig::load({"config=config/network.conf"}, config, errors))
{
    return 1;
}

auto runtime_logger = utils::create_async_logger("runtime", config.log.runtime);
auto user_logger = utils::create_async_logger("user", config.log.user);

coro::Runtime runtime(4, runtime_logger);        // four io_uring workers
if (not runtime.start_network(config))
{
    return 1;
}

runtime.block_on(co_main(user_logger, runtime.network_address()));
```

### API at a glance

| Type | Main interface |
| --- | --- |
| `coro::Runtime` | `start_network(config)`, `block_on(task)`, `spawn(task)`, `network_address()` |
| `coro::WaitGroup` | `add(n)`, `done()`, `co_await group` |
| `coro::net::UdpSocket` | `bind(addr)`, `send_to(...)`, `receive_from(span)`, `close()` |
| `coro::net::TcpListener` | `bind(addr)`, `accept()`, `close()` |
| `coro::net::TcpSocket` | `connect(addr)`, `read(span)`, `write(data)`, `write_all(data)`, `shutdown(how)`, `split()` |
| `TcpReadStream` / `TcpWriteStream` | `read(span)`, `write(data)`, `write_all(data)`, `shutdown()` |

Most socket operations return `std::expected<T, std::error_code>`; test with `if (not result)` and inspect `result.error()`.

## Features

**Coroutine runtime**

- Lazy `co_async<T>` tasks (`initial_suspend` suspends, `final_suspend` symmetrically transfers back to the caller)
- `coro::spawn` posts a task to a worker, `coro::sleep` provides a cancellable timer
- `coro::WaitGroup` joins concurrent tasks; waiter registration and counter drop happen under one lock, so wakeups are never lost
- `coro::Runtime` owns the workers, wires up the network controller, and offers a `block_on()` entry point

**io_uring multi-threaded scheduler**

- Each worker owns one `io_uring` instance and one `eventfd` wakeup channel
- Timers live in a min-heap; the wait timeout follows the nearest deadline and falls back to 1 ms polling when idle
- Ready tasks are resumed in batches; cross-thread submissions use a locked queue plus `eventfd`
- Workers are `std::jthread`s driven by `stop_token`, so shutdown is graceful

**Packet capture and frame distribution**

- `AF_PACKET / SOCK_RAW` bound to a single NIC with `ETH_P_ALL`, non-blocking
- `PACKET_FANOUT_HASH` spreads one NIC's traffic across the capture fds of all workers
- `PACKET_IGNORE_OUTGOING` (plus `PACKET_FANOUT_FLAG_IGNORE_OUTGOING`) keeps our own frames out of the receive path
- The capture fd is armed with io_uring `poll_add`, so a frame directly schedules the decoding coroutine

**Protocol stack**

- Ethernet II framing with EtherType dispatch
- ARP request/reply, address mapping table with timeout-based eviction and lazy cleanup, MAC resolution awaiter
- IPv4: header checksum, TTL, protocol dispatch, fragmentation and reassembly (overlapping fragments are rejected), MTU-aware fragmentation on transmit
- ICMPv4: echo request/reply
- UDP: MTU-based fragmentation, port binding, per-socket receive queue and waiter wakeup
- TCP: full state machine, handshake, sliding send/receive windows, retransmission, zero-window probing, TIME_WAIT, half-close, SYN cookies
- Buffers: fixed-size frame pool plus reference-counted `SocketBuffer`; layers pass references instead of copying payloads

## Architecture

```
                     Application coroutines (co_await)
                                  |
    UdpSocket / TcpSocket / TcpListener / TcpReadStream / TcpWriteStream
                                  |  awaiters: bind / connect / accept / send / receive / read / write / close
                                  v
                     SocketRegistry (NetworkController)
                                  |
        +--------------+----------+---------------+
        v              v                          v
   UdpManager      TcpManager                ICMPv4Manager
                   TcpSession x N
        +--------------+--------------------------+
                       v
                  Ipv4Manager              <- fragmentation, checksum, TTL
                       v
                  LinkManager              <- ARP table, MAC resolution
                       v
              Ethernet frame codec
                       v
             AF_PACKET (eth0) + io_uring poll
                       v
              Polling worker x N (one ring per thread)
```

### Receive path

1. io_uring reports the capture fd readable and `Polling` schedules `NetworkController::make_task_for_socket_packet()`
2. `recvfrom` copies one complete Ethernet frame into a `SocketBuffer` (a frame-pool slot is returned automatically when the last reference dies)
3. `LinkManager` parses the Ethernet header and dispatches on EtherType to ARP or IPv4
4. `Ipv4Manager` validates the header, reassembles fragments if needed, and dispatches on protocol number to ICMP / UDP / TCP
5. UDP enqueues the datagram on the socket matching the 4-tuple and wakes its waiter; TCP finds the session for the 4-tuple and feeds the state machine
6. Session results (connected, readable, peer closed) are handed back through `notify_user()`, which resumes the suspended coroutine

### Transmit path

1. The application awaits `socket.write(...)` or `send_to(...)`; the awaiter passes the request to the matching manager
2. The transport layer fills in headers, splits the payload by MTU, and computes checksums
3. `Ipv4Manager` writes its header and fragments if necessary; the destination MAC comes from the ARP table, and a miss suspends the caller while an ARP request is sent asynchronously
4. `LinkManager` writes the Ethernet header and `sendto`s the frame into the `AF_PACKET` socket

## Directory Layout

```
async-network/
├── CMakeLists.txt
├── main.cpp                     # sample: UDP echo + TCP echo
├── config/network.conf          # default configuration (copied to the build output)
├── include/async/               # public headers (coro.h / socket.h)
├── src/
│   ├── async/                   # coroutine runtime
│   │   ├── task.hpp             # co_async<T> task and awaiter
│   │   ├── spawn.hpp            # spawn abstraction
│   │   ├── runtime.{h,cpp}      # Runtime: worker management, block_on
│   │   ├── polling.{h,cpp}      # io_uring event loop and packet dispatch
│   │   ├── timer.{h,cpp}        # timer / SleepAwaiter
│   │   └── wait_group.{h,cpp}   # WaitGroup
│   ├── net/
│   │   ├── protocol/
│   │   │   ├── network_controller.{h,cpp}   # stack facade implementing every layer service
│   │   │   ├── capability/                  # layer abstractions (physical/link/internet/transport/application)
│   │   │   ├── header/                      # ethernet/arp/ipv4/tcp/tcp_option/udp/icmpv4 codecs
│   │   │   └── manager/
│   │   │       ├── link_manager.*           # ARP table
│   │   │       ├── internet/                # ipv4_manager, icmpv4_manager
│   │   │       └── transport/               # udp_manager, tcp_manager, tcp_session, send/receive windows
│   │   └── common/                          # config, addresses, checksum, SocketBuffer, frame pool
│   ├── socket/                  # application-facing async sockets and awaiters
│   └── utils/                   # async logger
└── tests/                       # gtest unit tests
```

## Requirements

- Linux (developed and verified on **WSL2 / Ubuntu**)
- A compiler with complete C++23 coroutine and `std::expected` support: GCC 13+ or Clang 17+
- CMake (`CMakeLists.txt` currently declares a minimum of 4.3)
- `liburing` (located through `pkg-config`) and `pthread`
- `spdlog` and `googletest` are vendored under `third_part/`
- Packet capture needs root or `CAP_NET_RAW`

```bash
sudo apt update
sudo apt install -y build-essential cmake pkg-config liburing-dev
```

## Build and Run

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

```bash
# Reads config/network.conf by default and listens on the configured host ip
sudo ./build/async_network

# Override configuration entries on the command line
sudo ./build/async_network config=config/network.conf interface=eth0 ip=192.168.1.100

# Unit tests
./build/async_network_ut --gtest_color=yes
```

## Capturing eth0 under WSL2

The sample targets `eth0`. The `ip` in the configuration file is the stack's own address, in the same subnet as the current IPv4 address of that NIC.

```bash
# 1. Find the WSL2 virtual NIC address
ip -4 addr show dev eth0
#    inet 172.31.208.5/20 ...   -> ip=172.31.208.5

# 2. Update the configuration (or pass it on the command line)
sed -i 's/^ip=.*/ip=172.31.208.5/' config/network.conf

# 3. Run as root (AF_PACKET needs CAP_NET_RAW)
sudo ./build/async_network
#    To avoid sudo, grant the capability instead:
sudo setcap cap_net_raw=eip ./build/async_network && ./build/async_network
```

Notes for testing:

- The kernel also owns the `eth0` address and processes these frames, but it does not recognise the
  stack's custom IP, so those packets are dropped by the system.

- Connections from **inside WSL2** to that address travel over `lo`, so the user-space stack never sees
  them. Test from the Windows host or from another machine on the same subnet (in NAT mode the host can
  reach the WSL2 address directly):

  ```powershell
  Test-NetConnection 172.31.208.5 -Port 7891
  ```

  ```bash
  # from another Linux host
  echo quit | nc -u 172.31.208.5 7890
  nc 172.31.208.5 7891
  ```

- If the WSL2 virtual NIC uses checksum offload, received frames may carry invalid checksums and get
  dropped. For debugging only, disable validation with `ipv4.checksum_check=off` and
  `tcp.checksum_check=off`.

## TCP Implementation Notes

- Eleven states: `Listen / SynReceived / SynSent / Established / FinWait1 / FinWait2 / CloseWait /
  Closing / LastAck / TimeWait / Closed`, each with its own inbound segment handler (`tcp_session_impl.cpp`)
- Handshake: active and passive open, advertise the local MSS, clamp outgoing segments to the peer MSS,
  optional stateless SYN cookie handshake (rotating time slots, peer MSS encoded in the low cookie bits)
- Send side: sliding window with an in-flight queue, cumulative-ACK retransmission on timeout, exponential
  RTO backoff capped by `max_rto_ms`
- Receive side: next expected sequence plus out-of-order reassembly, window updates when the window
  drains, persist probes when the peer advertises zero window
- Close: half-close via `shutdown(Read/Write/Both)`, FIN in both directions, 2MSL TIME_WAIT timer
- Hardening: challenge ACK on in-window RST, lazy expiry and a bound on the accept queue, rate-limited drop logs
- Options: MSS / window scale / SACK permitted / timestamp are decoded; only MSS is sent during the handshake
- Checksums: TCP pseudo-header checksum, validation can be disabled for debugging only

## TODO

### TCP

- **Fast retransmit and fast recovery**: there is no duplicate-ACK counting today, so loss is only
  recovered by the RTO timer. Add triple-duplicate-ACK detection and SACK-based loss recovery.
- **Congestion control**: the send window is the advertised window with no `cwnd`/`ssthresh`, so slow
  start, congestion avoidance and algorithms such as CUBIC or BBR are missing; throughput on lossy links
  is limited.
- **RTT estimation**: the RTO only doubles from a fixed initial value. Measure SRTT/RTTVAR (RFC 6298)
  and derive an adaptive RTO with Karn's algorithm.
- **Window scale and timestamp negotiation**: the options are decoded but never offered in the handshake,
  which caps the usable window at 64 KB.
- **Delayed ACK, Nagle, keepalive, urgent data**: none of these behaviours exist yet.
- **Per-connection buffer limits and backpressure**: window sizes are fixed from configuration, and
  queued outbound segments are unbounded.
- **Cancellable I/O**: a blocked read/write cannot be cancelled or given a timeout.

### Link and internet layers

- **IPv4 only**: no IPv6/NDP, DHCP, routing table or multi-NIC support.
- **Fragment reassembly timeout and table cap**: entries currently disappear only when the datagram
  completes, so a hostile or lossy peer can pin memory.
- **Path MTU discovery and ICMP error propagation**: only echo is answered; Destination Unreachable and
  Time Exceeded are not surfaced to sessions.


## License

[Apache-2.0](LICENSE)
