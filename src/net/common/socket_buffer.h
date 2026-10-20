#ifndef ASYNC_NETWORK_SOCKET_BUFFER_H
#define ASYNC_NETWORK_SOCKET_BUFFER_H

#include <cstddef>
#include <deque>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

namespace coro::net {

enum class Dir { Forward, Backward };

template<typename T>
class SocketBufferIterator {
public:
    static_assert(std::is_same_v<T, uint8_t> || std::is_same_v<T, const uint8_t>, "T must be uint8 or const uint8");
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = T*;
    using reference = T&;
    using span_list = std::conditional_t<std::is_const_v<T>,
        std::vector<std::span<const uint8_t>>,
        std::vector<std::span<uint8_t>>>;

    explicit SocketBufferIterator(span_list spans = {})
        : spans(std::move(spans))
    {
        seek();
    }

    reference operator*() const
    {
        return current_span[offset];
    }

    pointer operator->() const
    {
        return current_span.data() + offset;
    }

    SocketBufferIterator& operator++()
    {
        if (++offset >= current_span.size())
        {
            ++span_index;
            seek();
        }
        return *this;
    }

    SocketBufferIterator operator++(int)
    {
        SocketBufferIterator previous = *this;
        ++(*this);
        return previous;
    }

    friend bool operator==(const SocketBufferIterator& lhs, const SocketBufferIterator& rhs)
    {
        if (lhs.spans.empty() || rhs.spans.empty())
        {
            return lhs.spans.empty() && rhs.spans.empty();
        }
        return lhs.current_span.data() == rhs.current_span.data() && lhs.offset == rhs.offset;
    }

    friend bool operator!=(const SocketBufferIterator& lhs, const SocketBufferIterator& rhs)
    {
        return not(lhs == rhs);
    }

private:
    void seek()
    {
        while (span_index < spans.size())
        {
            const std::span<T> data = spans[span_index];
            if (not data.empty())
            {
                current_span = data;
                offset = 0;
                return;
            }
            ++span_index;
        }

        spans = {};
        current_span = {};
        span_index = 0;
        offset = 0;
    }

    span_list spans;
    std::size_t span_index = 0;
    std::size_t offset = 0;
    std::span<T> current_span;
};


class SocketBufferInterface {
public:
    using iterator = SocketBufferIterator<uint8_t>;
    using const_iterator = SocketBufferIterator<const uint8_t>;

    virtual ~SocketBufferInterface() = default;
    virtual size_t consume(size_t n, Dir dir = Dir::Backward) = 0;
    virtual std::span<uint8_t> get_header_payload() const = 0;
    virtual std::span<uint8_t> allocate(std::size_t size) = 0;
    virtual void reclaim() = 0;
    virtual std::shared_ptr<SocketBufferInterface> clone() const = 0;

    virtual size_t size() const;
    virtual int get_sequence() const;
    virtual void set_sequence(int s);

    virtual std::vector<std::span<uint8_t>> get_spans() = 0;
    virtual std::vector<std::span<const uint8_t>> get_spans() const = 0;

    iterator begin();
    iterator end();
    const_iterator begin() const;
    const_iterator end() const;
    const_iterator cbegin() const;
    const_iterator cend() const;
    size_t copy_to(std::span<uint8_t> dst) const;

protected:
    int sequence = 0;
};

class SocketBuffer {
public:
    SocketBuffer();
    SocketBuffer(std::nullptr_t);
    explicit SocketBuffer(std::shared_ptr<SocketBufferInterface> impl);
    SocketBuffer(const SocketBuffer& other);
    SocketBuffer(SocketBuffer&& other) noexcept = default;
    SocketBuffer& operator=(const SocketBuffer& other);
    SocketBuffer& operator=(SocketBuffer&& other) noexcept = default;

    static SocketBuffer reuse_for_outbound(const SocketBuffer& inbound);
    static SocketBuffer make_outbound_view(uint8_t* ptr, size_t capacity);
    SocketBuffer clone() const;
    SocketBufferInterface* operator->();
    SocketBufferInterface& operator*();
    const SocketBufferInterface* operator->() const;
    const SocketBufferInterface& operator*() const;
    explicit operator bool() const;
    void reset();

    class Storage {
    public:
        Storage(uint8_t* ptr, size_t capacity, std::function<void(const void*)> recycle);
        ~Storage();

        Storage(const Storage&) = delete;
        Storage& operator=(const Storage&) = delete;

        uint8_t* ptr;
        const size_t capacity;
        std::function<void(const void*)> recycle;
    };

private:
    std::shared_ptr<SocketBufferInterface> impl = nullptr;
};

class SocketBufferImpl : public SocketBufferInterface {
public:
    SocketBufferImpl(uint8_t* ptr, size_t capacity, std::optional<size_t> len,
                     const std::function<void(const void*)>& recycle = nullptr);
    SocketBufferImpl(std::shared_ptr<SocketBuffer::Storage> storage, std::span<uint8_t> payload);

    std::shared_ptr<SocketBufferInterface> clone() const override;
    size_t size() const override;
    std::span<uint8_t> allocate(std::size_t size) override;
    size_t consume(size_t n, Dir dir = Dir::Backward) override;
    std::span<uint8_t> get_header_payload() const override;
    void reclaim() override;

    std::vector<std::span<uint8_t>> get_spans() override;
    std::vector<std::span<const uint8_t>> get_spans() const override;

private:
    std::shared_ptr<SocketBuffer::Storage> storage;
    std::span<uint8_t> payload;
};

class BulkSocketBufferImpl : public SocketBufferInterface {
public:
    void add(const SocketBuffer& buf);

    std::shared_ptr<SocketBufferInterface> clone() const override;
    size_t size() const override;
    size_t consume(size_t n, Dir dir = Dir::Backward) override;
    std::span<uint8_t> get_header_payload() const override;
    std::span<uint8_t> allocate(std::size_t size) override;
    void reclaim() override;

    std::vector<std::span<uint8_t>> get_spans() override;
    std::vector<std::span<const uint8_t>> get_spans() const override;

private:
    std::deque<SocketBuffer> buffer;
};

}
#endif //ASYNC_NETWORK_SOCKET_BUFFER_H
