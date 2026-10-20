# async-network

[English](README.md) | **简体中文**

基于 **C++23 协程**、**多线程 io_uring 调度器** 与 **AF_PACKET 抓包** 的用户态网络协议栈。

程序不经过内核 TCP/IP 协议栈：它直接从 Linux 虚拟网卡（WSL2 中的 `eth0`）收以太网帧，自行完成
Ethernet / ARP / IPv4 / ICMPv4 / UDP / TCP 的解析与封装，再通过同一块网卡把帧发回去。对上则暴露一套
`co_await` 风格的异步 socket API，让应用代码像写同步逻辑一样写网络程序。

```cpp
auto listener = co_await TcpListener::bind("192.168.1.100:7891");
auto client = co_await listener.accept();
co_await client.write("hello");
```

## 使用示例

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
        auto accepted = co_await listener.accept();   // 挂起到三次握手完成
        if (not accepted)
        {
            break;
        }

        auto socket = std::move(accepted.value());
        auto [rx, tx] = std::move(socket).split();    // 读、写方向可独立并发
        coro::spawn(echo_loop(std::move(rx), std::move(tx)));
    }
}
```

### 启动运行时

```cpp
NetConfig config;
std::vector<std::string> errors;
if (not NetConfig::load({"config=config/network.conf"}, config, errors))
{
    return 1;
}

auto runtime_logger = utils::create_async_logger("runtime", config.log.runtime);
auto user_logger = utils::create_async_logger("user", config.log.user);

coro::Runtime runtime(4, runtime_logger);        // 4 个 io_uring worker
if (not runtime.start_network(config))
{
    return 1;
}

runtime.block_on(co_main(user_logger, runtime.network_address()));
```

### 接口速览

| 类型 | 主要接口 |
| --- | --- |
| `coro::Runtime` | `start_network(config)`、`block_on(task)`、`spawn(task)`、`network_address()` |
| `coro::WaitGroup` | `add(n)`、`done()`、`co_await group` |
| `coro::net::UdpSocket` | `bind(addr)`、`send_to(...)`、`receive_from(span)`、`close()` |
| `coro::net::TcpListener` | `bind(addr)`、`accept()`、`close()` |
| `coro::net::TcpSocket` | `connect(addr)`、`read(span)`、`write(data)`、`shutdown(how)`、`split()` |
| `TcpReadStream` / `TcpWriteStream` | `read(span)`、`write(data)`、`shutdown()` |

大多数 socket 操作返回 `std::expected<T, std::error_code>`，用 `if (not result)` 与 `result.error()` 处理失败。

## 特性

**协程运行时**

- 惰性 `co_async<T>` 任务（`initial_suspend` 挂起、`final_suspend` 对称转移回调用者）
- `coro::spawn` 把任务投递到工作线程，`coro::sleep` 提供可取消的定时器
- `coro::WaitGroup` 用于并发任务汇合，等待者的唤醒与计数归零在同一把锁内完成，不会丢唤醒
- 网络运行时 `coro::Runtime` 负责创建 worker、初始化网络控制器与 `block_on()` 阻塞入口

**io_uring 多线程调度器**

- 每个 worker 独占一个 `io_uring` 实例与一个 `eventfd` 唤醒通道
- 定时器为最小堆，等待超时按最近截止时间动态计算；无定时器时回退到 1 ms 轮询
- 就绪任务批处理执行，跨线程投递通过加锁队列 + `eventfd` 唤醒
- 工作线程基于 `std::jthread` + `stop_token`，支持优雅停止

**抓包与帧分发**

- `AF_PACKET / SOCK_RAW` 绑定指定网卡，`ETH_P_ALL` 收全部以太网帧，非阻塞
- `PACKET_FANOUT_HASH` 把一个网卡的流量按流哈希分发到多个 worker 的抓包 fd
- `PACKET_IGNORE_OUTGOING`（含 `PACKET_FANOUT_FLAG_IGNORE_OUTGOING`）避免抓到自己发出的帧
- 抓包 fd 注册进 io_uring `poll_add`，帧到达即调度对应协程，无需额外轮询线程

**协议栈**

- Ethernet II 帧的收发与 EtherType 分发
- ARP 请求/应答、地址映射表（超时淘汰 + 懒清理）、MAC 解析 awaiter
- IPv4：首部校验、TTL、协议分发、分片与重组（重叠分片直接丢弃）、按 MTU 分片发送
- ICMPv4：回显请求应答
- UDP：按 MTU 分片发送、端口绑定、每 socket 收包队列与等待者唤醒
- TCP：完整状态机、握手、滑动收发窗口、重传、零窗口探测、TIME_WAIT、半关闭、SYN Cookie
- 缓冲区：固定帧池 + 引用计数 `SocketBuffer`，跨层传递只传引用，不复制载荷

## 架构

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

### 收包路径

1. io_uring 报告抓包 fd 可读，`Polling` 调度 `NetworkController::make_task_for_socket_packet()`
2. `recvfrom` 读出一个完整以太网帧并包装成 `SocketBuffer`（最后一个引用释放时自动归还帧池槽位）
3. `LinkManager` 解析以太网首部，按 EtherType 交给 ARP 或 IPv4
4. `Ipv4Manager` 校验首部、必要时重组分片，按协议号交给 ICMP / UDP / TCP
5. UDP 按四元组投递到对应 socket 队列并唤醒等待者；TCP 按四元组找到会话并送入状态机
6. 会话结果（连接建立、可读、对端关闭）通过 `notify_user()` 交回，恢复挂起的协程

### 发包路径

1. 应用 `co_await socket.write(...)` 或 `send_to(...)`，awaiter 把请求交给对应的管理器
2. 传输层填充首部、按 MTU 拆分载荷、计算校验和
3. `Ipv4Manager` 填首部、必要时分片；目的 MAC 查 ARP 表，未命中则挂起调用者并异步发送 ARP 请求
4. `LinkManager` 填以太网首部，`sendto` 写入 `AF_PACKET` socket

## 目录结构

```
async-network/
├── CMakeLists.txt
├── main.cpp                     # 示例：UDP echo + TCP echo
├── config/network.conf          # 默认配置（构建时复制到输出目录）
├── include/async/               # 对外头文件（coro.h / socket.h）
├── src/
│   ├── async/                   # 协程运行时
│   │   ├── task.hpp             # co_async<T> 任务与 awaiter
│   │   ├── spawn.hpp            # spawn 抽象接口
│   │   ├── runtime.{h,cpp}      # Runtime：worker 管理、block_on
│   │   ├── polling.{h,cpp}      # io_uring 事件循环 + 抓包分发
│   │   ├── timer.{h,cpp}        # 定时器 / SleepAwaiter
│   │   └── wait_group.{h,cpp}   # WaitGroup
│   ├── net/
│   │   ├── protocol/
│   │   │   ├── network_controller.{h,cpp}   # 协议栈门面，实现各层服务接口
│   │   │   ├── capability/                  # 各层服务抽象（physical/link/internet/transport/application）
│   │   │   ├── header/                      # ethernet/arp/ipv4/tcp/tcp_option/udp/icmpv4 报文编解码
│   │   │   └── manager/
│   │   │       ├── link_manager.*           # ARP 表
│   │   │       ├── internet/                # ipv4_manager、icmpv4_manager
│   │   │       └── transport/               # udp_manager、tcp_manager、tcp_session、收发窗口
│   │   └── common/                          # 配置、地址、校验和、SocketBuffer、帧池
│   ├── socket/                  # 面向应用的异步 socket 与 awaiter
│   └── utils/                   # 异步 logger
└── tests/                       # gtest 单元测试
```

## 环境要求

- Linux（开发与验证环境为 **WSL2 / Ubuntu**）
- 支持完整 C++23 协程与 `std::expected` 的编译器：GCC 13+ 或 Clang 17+
- CMake（`CMakeLists.txt` 声明最低 4.3）
- `liburing`（经 `pkg-config` 查找）与 `pthread`
- `spdlog`、`googletest` 已随仓库以源码形式放在 `third_part/` 下
- 抓包需要 root 或 `CAP_NET_RAW` 权限

```bash
sudo apt update
sudo apt install -y build-essential cmake pkg-config liburing-dev
```

## 构建与运行

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

```bash
# 默认读取 config/network.conf，监听配置中的 host ip
sudo ./build/async_network

# 命令行覆盖配置项
sudo ./build/async_network config=config/network.conf interface=eth0 ip=192.168.1.100

# 单元测试
./build/async_network_ut --gtest_color=yes
```

## 在 WSL2 中捕获 eth0

示例按 `eth0` 设计。配置文件里的 `ip` 是协议栈自定义地址，与该网卡当前的 IPv4 地址同一网段。

```bash
# 1. 查看 WSL2 虚拟网卡地址
ip -4 addr show dev eth0
#    inet 172.31.208.5/20 ...   -> ip=172.31.208.5

# 2. 修改配置（或命令行传入）
sed -i 's/^ip=.*/ip=172.31.208.5/' config/network.conf

# 3. 以 root 运行（AF_PACKET 需要 CAP_NET_RAW）
sudo ./build/async_network
#    不想用 sudo 时可以授予能力：
sudo setcap cap_net_raw=eip ./build/async_network && ./build/async_network
```

测试时请注意：

- 内核同样持有 `eth0` 的地址并会处理这些报文，但自定义 IP 不会被内核识别，从而被系统丢弃。

- 从 **WSL2 内部** 连接该地址走的是 `lo`，用户态协议栈看不到，因此请从 Windows 宿主机或同网段的
  其他机器发起测试（WSL2 NAT 模式下宿主机可直接访问该地址）：

  ```powershell
  Test-NetConnection 172.31.208.5 -Port 7891
  ```

  ```bash
  # 其他 Linux 主机
  echo quit | nc -u 172.31.208.5 7890
  nc 172.31.208.5 7891
  ```

- 若 WSL2 虚拟网卡存在校验和卸载，收到的帧可能带无效校验和而被丢弃，可在配置中临时关闭校验：
  `ipv4.checksum_check=off`、`tcp.checksum_check=off`（仅用于调试）。

## TCP 实现细节

- 11 个状态：`Listen / SynReceived / SynSent / Established / FinWait1 / FinWait2 / CloseWait /
  Closing / LastAck / TimeWait / Closed`，每个状态独立处理入站段（`tcp_session_impl.cpp`）
- 握手：主动/被动打开，通告本端 MSS，按对端 MSS 限制发送段大小；可选无状态 SYN Cookie 握手
  （时间片轮换 + 对端 MSS 编码进 cookie 低位）
- 发送：滑动窗口 + 在途段队列，超时按累计确认重传，RTO 指数退避并受 `max_rto_ms` 限制
- 接收：预期序号 + 乱序段缓存重组，窗口不足时发送窗口更新，窗口为 0 时启动 persist probe
- 关闭：支持半关闭（`shutdown(Read/Write/Both)`），FIN 双向关闭，TIME_WAIT 定时器 2MSL
- 防护：窗口内 RST 触发 challenge ACK，accept 队列懒过期 + 上限，丢弃日志限频
- 选项：MSS / 窗口扩大 / SACK permitted / 时间戳的编解码，握手中只发送 MSS
- 校验：TCP 校验和（伪首部）可配置关闭，仅用于调试

## TODO

### TCP

- **快速重传与快速恢复**：当前没有重复 ACK 计数，丢包只能靠 RTO 定时器恢复。需要加入三次重复 ACK
  检测与基于 SACK 的恢复。
- **拥塞控制**：发送窗口就是通告窗口，没有 `cwnd`/`ssthresh`，慢启动、拥塞避免以及 CUBIC、BBR 等
  算法均缺失，高丢包链路上吞吐受限。
- **RTT 估计**：RTO 仅从固定初值翻倍。需要测量 SRTT/RTTVAR（RFC 6298），结合 Karn 算法得到自适应
  RTO。
- **窗口扩大与时间戳协商**：选项只做了解码，握手阶段从未携带，可用窗口被限制在 64 KB。
- **延迟 ACK、Nagle、keepalive、紧急数据**：这些行为均未实现。
- **每连接缓冲上限与背压**：窗口大小取自配置且固定，已排队的发送段数量没有上限。
- **可取消 I/O**：阻塞的 read/write 无法取消或设置超时。

### 链路与网络层

- **仅 IPv4**：无 IPv6/NDP、DHCP、路由表与多网卡支持。
- **分片重组超时与表上限**：表项只在数据报重组完成时释放，恶意或丢包的对端可长期占用内存。
- **Path MTU 发现与 ICMP 差错上送**：只应答 Echo；Destination Unreachable 与 Time Exceeded 未上报给
  会话。


## License

[Apache-2.0](LICENSE)
