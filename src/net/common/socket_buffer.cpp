#include "socket_buffer.h"
#include <algorithm>
#include <utility>

namespace coro::net {

namespace {

template<typename Span>
std::vector<Span> collect_spans(const std::vector<SocketBuffer>& buffers)
{
    std::vector<Span> spans;
    spans.reserve(buffers.size());

    for (const auto& child: buffers)
    {
        spans.push_back(child->get_header_payload());
    }

    return spans;
}

}

size_t SocketBufferInterface::size() const
{
    return 0;
}

int SocketBufferInterface::get_sequence() const
{
    return sequence;
}

void SocketBufferInterface::set_sequence(int s)
{
    sequence = s;
}

SocketBufferInterface::iterator SocketBufferInterface::begin()
{
    return iterator(get_spans());
}

SocketBufferInterface::iterator SocketBufferInterface::end()
{
    return iterator{};
}

SocketBufferInterface::const_iterator SocketBufferInterface::begin() const
{
    return cbegin();
}

SocketBufferInterface::const_iterator SocketBufferInterface::end() const
{
    return cend();
}

SocketBufferInterface::const_iterator SocketBufferInterface::cbegin() const
{
    return const_iterator(get_spans());
}

SocketBufferInterface::const_iterator SocketBufferInterface::cend() const
{
    return const_iterator{};
}

size_t SocketBufferInterface::copy_to(std::span<uint8_t> dst) const
{
    size_t copied = 0;

    for (const std::span<const uint8_t> source: get_spans())
    {
        if (copied >= dst.size())
        {
            break;
        }

        const size_t len = std::min(source.size(), dst.size() - copied);
        std::copy_n(source.begin(), len, dst.begin() + copied);

        copied += len;
    }

    return copied;
}

SocketBufferStorage::SocketBufferStorage(uint8_t* ptr, size_t capacity, std::function<void(const void*)> recycle)
    : ptr(ptr),
      capacity(capacity),
      recycle(std::move(recycle))
{
}

SocketBufferStorage::~SocketBufferStorage()
{
    if (recycle)
        recycle(ptr);
}

SocketBufferImpl::SocketBufferImpl(uint8_t* ptr, size_t capacity, std::optional<size_t> len,
                                   std::function<void(const void*)> recycle)
    : storage(std::make_shared<SocketBufferStorage>(ptr, capacity, std::move(recycle)))
{
    if (len.has_value())
    {
        payload = std::span(ptr, len.value());
    }
    else
    {
        reclaim();
    }
}

SocketBufferImpl::SocketBufferImpl(std::shared_ptr<SocketBufferStorage> storage, std::span<uint8_t> payload)
    : storage(std::move(storage)),
      payload(payload)
{
}

size_t SocketBufferImpl::size() const
{
    return payload.size();
}

std::span<uint8_t> SocketBufferImpl::allocate(std::size_t size)
{
    if (payload.data() - storage->ptr < size)
    {
        return std::span<uint8_t>{};
    }

    uint8_t* start = payload.data() - size;
    payload = std::span(start, payload.size() + size);
    return std::span(start, size);
}

size_t SocketBufferImpl::consume(size_t n, Dir dir)
{
    int count = std::min(n, payload.size());
    if (dir == Dir::Backward)
    {
        payload = payload.subspan(count);
    }
    else
    {
        payload = payload.subspan(0, payload.size() - count);
    }

    return count;
}

std::span<uint8_t> SocketBufferImpl::get_header_payload() const
{
    return payload;
}

void SocketBufferImpl::reclaim()
{
    payload = std::span(storage->ptr + storage->capacity, 0);
}

std::vector<std::span<uint8_t>> SocketBufferImpl::get_spans()
{
    return {payload};
}

std::vector<std::span<const uint8_t>> SocketBufferImpl::get_spans() const
{
    return {payload};
}

BulkSocketBufferImpl::BulkSocketBufferImpl(const BulkSocketBufferImpl& other) = default;

BulkSocketBufferImpl::BulkSocketBufferImpl(BulkSocketBufferImpl&& other) noexcept = default;

BulkSocketBufferImpl& BulkSocketBufferImpl::operator=(const BulkSocketBufferImpl& other) = default;

BulkSocketBufferImpl& BulkSocketBufferImpl::operator=(BulkSocketBufferImpl&& other) noexcept = default;

BulkSocketBufferImpl::~BulkSocketBufferImpl() = default;

void BulkSocketBufferImpl::add(const SocketBuffer& buf)
{
    buffer.push_back(buf);
    std::ranges::sort(buffer, [](const SocketBuffer& lhs, const SocketBuffer& rhs) {
        return lhs->get_sequence() < rhs->get_sequence();
    });
}

size_t BulkSocketBufferImpl::size() const
{
    size_t size = 0;
    for (const auto& in: buffer)
    {
        size += in->size();
    }
    return size;
}

size_t BulkSocketBufferImpl::consume(size_t n, Dir dir)
{
    size_t count = n;
    const size_t size = buffer.size();
    for (size_t i = 0; i < size; i++)
    {
        if (dir == Dir::Backward)
        {
            auto& buf = buffer.at(i);
            count -= buf->consume(count);
        }
        else
        {
            auto& buf = buffer.at(size - 1 - i);
            count -= buf->consume(count, dir);
        }

        if (count == 0)
        {
            break;
        }
    }

    return n - count;
}

std::span<uint8_t> BulkSocketBufferImpl::get_header_payload() const
{
    return buffer.empty() ? std::span<uint8_t>{} : buffer.front()->get_header_payload();
}

std::span<uint8_t> BulkSocketBufferImpl::allocate(std::size_t size)
{
    return std::span<uint8_t>{};
}

void BulkSocketBufferImpl::reclaim()
{
    buffer.clear();
}

std::vector<std::span<uint8_t>> BulkSocketBufferImpl::get_spans()
{
    return collect_spans<std::span<uint8_t>>(buffer);
}

std::vector<std::span<const uint8_t>> BulkSocketBufferImpl::get_spans() const
{
    return collect_spans<std::span<const uint8_t>>(buffer);
}

SocketBuffer::SocketBuffer() noexcept = default;

SocketBuffer::SocketBuffer(std::nullptr_t) noexcept
{
}

SocketBuffer::SocketBuffer(SocketBufferImpl impl)
    : impl(std::move(impl))
{
}

SocketBuffer::SocketBuffer(BulkSocketBufferImpl impl)
    : impl(std::move(impl))
{
}

SocketBuffer::SocketBuffer(const SocketBuffer& other) = default;

SocketBuffer::SocketBuffer(SocketBuffer&& other) noexcept
    : impl(std::move(other.impl))
{
    other.reset();
}

SocketBuffer& SocketBuffer::operator=(const SocketBuffer& other) = default;

SocketBuffer& SocketBuffer::operator=(SocketBuffer&& other) noexcept
{
    if (this != &other)
    {
        impl = std::move(other.impl);
        other.reset();
    }
    return *this;
}

SocketBuffer::~SocketBuffer() = default;

SocketBuffer SocketBuffer::reuse_for_outbound(const SocketBuffer& inbound)
{
    SocketBuffer outbound = inbound;
    if (outbound)
    {
        outbound->reclaim();
    }
    return outbound;
}

SocketBuffer SocketBuffer::make_outbound_view(uint8_t* ptr, size_t capacity)
{
    return SocketBuffer(SocketBufferImpl(ptr, capacity, std::nullopt));
}

SocketBuffer SocketBuffer::clone() const
{
    return *this;
}

SocketBufferInterface* SocketBuffer::operator->()
{
    if (auto* single = std::get_if<SocketBufferImpl>(&impl))
    {
        return single;
    }

    if (auto* bulk = std::get_if<BulkSocketBufferImpl>(&impl))
    {
        return bulk;
    }

    return nullptr;
}

SocketBufferInterface& SocketBuffer::operator*()
{
    return *operator->();
}

const SocketBufferInterface* SocketBuffer::operator->() const
{
    if (const auto* single = std::get_if<SocketBufferImpl>(&impl))
    {
        return single;
    }

    if (const auto* bulk = std::get_if<BulkSocketBufferImpl>(&impl))
    {
        return bulk;
    }

    return nullptr;
}

const SocketBufferInterface& SocketBuffer::operator*() const
{
    return *operator->();
}

SocketBuffer::operator bool() const
{
    return not std::holds_alternative<std::monostate>(impl);
}

void SocketBuffer::reset()
{
    impl.emplace<std::monostate>();
}

}
