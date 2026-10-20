#include "socket_buffer.h"
#include <algorithm>
#include <utility>

namespace coro::net {

namespace {

template<typename Span>
std::vector<Span> collect_spans(const std::deque<SocketBuffer>& buffers)
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

SocketBuffer::SocketBuffer()
{
}

SocketBuffer::SocketBuffer(std::nullptr_t)
{
}

SocketBuffer::SocketBuffer(std::shared_ptr<SocketBufferInterface> impl)
    : impl(std::move(impl))
{
}

SocketBuffer::SocketBuffer(const SocketBuffer& other)
    : impl(other.impl ? other.impl->clone() : nullptr)
{
}

SocketBuffer& SocketBuffer::operator=(const SocketBuffer& other)
{
    impl = other.impl ? other.impl->clone() : nullptr;
    return *this;
}

SocketBuffer SocketBuffer::reuse_for_outbound(const SocketBuffer& inbound)
{
    auto outbound = inbound;
    outbound->reclaim();
    return outbound;
}

SocketBuffer SocketBuffer::make_outbound_view(uint8_t* ptr, size_t capacity)
{
    auto outbound = SocketBuffer(std::make_shared<SocketBufferImpl>(ptr, capacity, std::nullopt));
    return outbound;
}

SocketBuffer SocketBuffer::clone() const
{
    return impl ? SocketBuffer(impl->clone()) : SocketBuffer{};
}

SocketBufferInterface* SocketBuffer::operator->()
{
    return impl.get();
}

SocketBufferInterface& SocketBuffer::operator*()
{
    return *impl;
}

const SocketBufferInterface* SocketBuffer::operator->() const
{
    return impl.get();
}

const SocketBufferInterface& SocketBuffer::operator*() const
{
    return *impl;
}

SocketBuffer::operator bool() const
{
    return impl != nullptr;
}

void SocketBuffer::reset()
{
    impl.reset();
}

SocketBuffer::Storage::Storage(uint8_t* ptr, size_t capacity, std::function<void(const void*)> recycle)
    : ptr(ptr),
      capacity(capacity),
      recycle(std::move(recycle))
{
}

SocketBuffer::Storage::~Storage()
{
    if (recycle)
        recycle(ptr);
}

SocketBufferImpl::SocketBufferImpl(uint8_t* ptr, size_t capacity, std::optional<size_t> len,
                                   const std::function<void(const void*)>& recycle)
    : storage(std::make_shared<SocketBuffer::Storage>(ptr, capacity, recycle))
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

SocketBufferImpl::SocketBufferImpl(std::shared_ptr<SocketBuffer::Storage> storage, std::span<uint8_t> payload)
    : storage(std::move(storage)),
      payload(payload)
{
}

std::shared_ptr<SocketBufferInterface> SocketBufferImpl::clone() const
{
    auto view = std::make_shared<SocketBufferImpl>(storage, payload);
    view->set_sequence(get_sequence());
    return view;
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

void BulkSocketBufferImpl::add(const SocketBuffer& buf)
{
    buffer.push_back(buf);
    std::ranges::sort(buffer, [](const SocketBuffer& lhs, const SocketBuffer& rhs) {
        return lhs->get_sequence() < rhs->get_sequence();
    });
}

std::shared_ptr<SocketBufferInterface> BulkSocketBufferImpl::clone() const
{
    auto view = std::make_shared<BulkSocketBufferImpl>();
    view->set_sequence(get_sequence());
    for (const auto& child: buffer)
    {
        view->buffer.push_back(child.clone());
    }
    return view;
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

}
