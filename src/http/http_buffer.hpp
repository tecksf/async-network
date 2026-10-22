#ifndef ASYNC_NETWORK_HTTP_BUFFER_HPP
#define ASYNC_NETWORK_HTTP_BUFFER_HPP

#include <cstddef>
#include <string>

namespace coro::net {

class HttpBuffer {
public:
    static constexpr size_t CompactThreshold = 8 * 1024;

    void append(std::string_view data)
    {
        storage.append(data);
    }

    std::string_view peek() const
    {
        return std::string_view(storage).substr(read_offset);
    }

    void consume(size_t count)
    {
        read_offset += count > size() ? size() : count;

        if (read_offset >= storage.size())
        {
            storage.clear();
            read_offset = 0;
            return;
        }

        if (read_offset >= CompactThreshold and read_offset * 2 >= storage.size())
        {
            storage.erase(0, read_offset);
            read_offset = 0;
        }
    }

    size_t size() const
    {
        return storage.size() - read_offset;
    }

    bool empty() const
    {
        return size() == 0;
    }

    void clear()
    {
        storage.clear();
        read_offset = 0;
    }

private:
    std::string storage;
    size_t read_offset = 0;
};

}

#endif //ASYNC_NETWORK_HTTP_BUFFER_HPP
