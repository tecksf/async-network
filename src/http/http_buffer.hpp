#ifndef ASYNC_NETWORK_HTTP_BUFFER_HPP
#define ASYNC_NETWORK_HTTP_BUFFER_HPP

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <span>
#include <string_view>
#include <vector>

namespace coro::net {

// Typical read flow (read straight into the buffer, no intermediate copy):
//   auto space = input.write_space();       // contiguous free tail space
//   if (space.empty()) { consume/compact, then retry }
//   read(space) == n;  input.commit(n);     // commit the bytes actually read
//   input.peek();                           // parse
//   input.consume(parsed);                  // drop the parsed prefix
//   input.compact();                        // move leftovers to the front, tail is free again
template <std::size_t InitialCapacity = 8 * 1024>
class BasicHttpBuffer {
public:
    using value_type = std::byte;
    using size_type = std::size_t;

    static constexpr size_type MinCapacity = InitialCapacity == 0 ? 1 : InitialCapacity;

    BasicHttpBuffer() = default;

    explicit BasicHttpBuffer(size_type initial_capacity)
    {
        reserve(initial_capacity);
    }

    void append(std::string_view data)
    {
        append(std::span<const std::byte>(reinterpret_cast<const std::byte*>(data.data()), data.size()));
    }

    void append(std::span<const std::byte> data)
    {
        if (data.empty())
        {
            return;
        }

        ensure_space(data.size());

        const size_type tail = wrap(head + count);
        const size_type first = std::min(data.size(), storage.size() - tail);
        std::memcpy(storage.data() + tail, data.data(), first);

        if (first < data.size())
        {
            std::memcpy(storage.data(), data.data() + first, data.size() - first);
        }

        count += data.size();
    }

    // Contiguous tail space that can be passed to read/recv directly.
    // An empty span means the buffer is full: consume or compact first.
    std::span<std::byte> write_space()
    {
        if (storage.empty())
        {
            relocate(MinCapacity);
        }

        if (spare() == 0)
        {
            return {};
        }

        const size_type tail = wrap(head + count);
        if (tail < head)
        {
            return { storage.data() + tail, head - tail };
        }

        return { storage.data() + tail, storage.size() - tail };
    }

    // Commit the bytes written directly through write_space().
    void commit(size_type length)
    {
        count += std::min(length, spare());
    }

    // Readable bytes: the ring is compacted to [0, size()) first when it wraps.
    std::string_view peek()
    {
        compact();

        if (count == 0)
        {
            return {};
        }

        return { reinterpret_cast<const char*>(storage.data() + head), count };
    }

    void consume(size_type amount)
    {
        const size_type consumed = std::min(amount, count);
        count -= consumed;

        if (count == 0)
        {
            head = 0;
            return;
        }

        head = wrap(head + consumed);
    }

    // Move the not yet consumed bytes to the front of storage, so the tail
    // becomes one large contiguous block again.
    void compact()
    {
        if (count == 0)
        {
            head = 0;
            return;
        }

        if (head == 0)
        {
            return;
        }

        if (head + count > storage.size())
        {
            // Wrapped data can only live in [0, head): rotate [head, end) to the front.
            std::ranges::rotate(storage, storage.begin() + static_cast<std::ptrdiff_t>(head));
        }
        else
        {
            std::memmove(storage.data(), storage.data() + head, count);
        }

        head = 0;
    }

    size_type size() const
    {
        return count;
    }

    bool empty() const
    {
        return count == 0;
    }

    size_type capacity() const
    {
        return storage.size();
    }

    size_type spare() const
    {
        return storage.size() - count;
    }

    void reserve(size_type wanted)
    {
        if (wanted > storage.size())
        {
            relocate(wanted);
        }
    }

    void clear()
    {
        count = 0;
        head = 0;
    }

private:
    size_type wrap(size_type index) const
    {
        const size_type slots = storage.size();
        if (slots == 0)
        {
            return 0;
        }

        while (index >= slots)
        {
            index -= slots;
        }
        return index;
    }

    // Copy the readable bytes in logical order (starting at head) to a contiguous destination.
    void copy_ordered(std::byte* destination) const
    {
        if (count == 0 or storage.empty())
        {
            return;
        }

        const size_type first = std::min(count, storage.size() - head);
        std::memcpy(destination, storage.data() + head, first);

        if (first < count)
        {
            std::memcpy(destination + first, storage.data(), count - first);
        }
    }

    void relocate(size_type wanted)
    {
        std::vector<std::byte> resized(std::max(wanted, MinCapacity));
        copy_ordered(resized.data());
        storage.swap(resized);
        head = 0;
    }

    void ensure_space(size_type extra)
    {
        if (spare() >= extra)
        {
            return;
        }

        size_type grown = storage.empty() ? MinCapacity : storage.size();
        while (grown < count + extra)
        {
            grown *= 2;
        }

        relocate(grown);
    }

    std::vector<std::byte> storage;
    size_type head = 0;   // read position
    size_type count = 0;  // number of stored bytes; write position = head + count
};

using HttpBuffer = BasicHttpBuffer<>;

}

#endif //ASYNC_NETWORK_HTTP_BUFFER_HPP
