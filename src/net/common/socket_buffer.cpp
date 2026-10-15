#include "socket_buffer.h"
#include <algorithm>
#include <utility>

namespace coro::net {

size_t SocketBufferInterface::size()
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
    return SocketBufferIterator<uint8_t>(this);
}

SocketBufferInterface::iterator SocketBufferInterface::end()
{
    return SocketBufferIterator<uint8_t>{};
}

SocketBufferInterface::const_iterator SocketBufferInterface::cbegin() const
{
    return SocketBufferIterator<const uint8_t>(this);
}

SocketBufferInterface::const_iterator SocketBufferInterface::cend() const
{
    return SocketBufferIterator<const uint8_t>{};
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

SocketBuffer SocketBuffer::clone() const
{
    return impl ? SocketBuffer(impl->clone()) : SocketBuffer{};
}

SocketBufferInterface* SocketBuffer::operator->() const
{
    return impl.get();
}

SocketBufferInterface& SocketBuffer::operator*() const
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

size_t SocketBufferImpl::size()
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

size_t SocketBufferImpl::get_span_count() const
{
    return 1;
}

std::span<uint8_t> SocketBufferImpl::get_span(int index) const
{
    return index == 0 ? payload : std::span<uint8_t>{};
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

size_t BulkSocketBufferImpl::size()
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
    return get_span(0);
}

std::span<uint8_t> BulkSocketBufferImpl::allocate(std::size_t size)
{
    return std::span<uint8_t>{};
}

void BulkSocketBufferImpl::reclaim()
{
    buffer.clear();
}

std::size_t BulkSocketBufferImpl::get_span_count() const
{
    return buffer.size();
}

std::span<uint8_t> BulkSocketBufferImpl::get_span(int index) const
{
    return index < buffer.size() ? buffer[index]->get_header_payload() : std::span<uint8_t>{};
}

}
