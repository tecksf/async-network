#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>
#include <common/socket_buffer.h>

using namespace ::testing;
using namespace coro::net;

namespace coro::ut {

std::vector<uint8_t> buffer_bytes(const SocketBufferInterface& buffer)
{
    std::vector<uint8_t> bytes;
    for (auto it = buffer.cbegin(); it != buffer.cend(); ++it)
    {
        bytes.push_back(*it);
    }
    return bytes;
}

std::vector<uint8_t> sequential_bytes(size_t size)
{
    std::vector<uint8_t> bytes;
    bytes.reserve(size);
    for (size_t i = 0; i < size; i++)
    {
        bytes.push_back(static_cast<uint8_t>(i));
    }
    return bytes;
}

SocketBuffer make_socket_buffer(uint8_t* data, size_t size, int sequence)
{
    SocketBuffer buffer(std::make_shared<SocketBufferImpl>(data, size, size));
    buffer->set_sequence(sequence);
    return buffer;
}

class FakeIteratorView : public SocketBufferIteratorView {
public:
    void append(std::span<uint8_t> span)
    {
        spans.push_back(span);
    }

    std::span<uint8_t> get_span(int index) const override
    {
        if (index < 0 || static_cast<size_t>(index) >= spans.size())
        {
            return std::span<uint8_t>{};
        }
        return spans[static_cast<size_t>(index)];
    }

    size_t get_span_count() const override
    {
        return spans.size();
    }

private:
    std::vector<std::span<uint8_t>> spans;
};

class SocketBufferTest : public ::testing::Test {
protected:
    static constexpr int Size = 32;

    void SetUp() override
    {
        EXPECT_FALSE(sut);

        auto recycle = [this](const void*) { called = true; };
        sut = SocketBuffer(std::make_shared<SocketBufferImpl>(data, Size, Size, recycle));
    }

    void TearDown() override
    {
        sut.reset();
        EXPECT_TRUE(called);
    }

    bool called = false;
    uint8_t data[Size];
    SocketBuffer sut;
};

TEST_F(SocketBufferTest, WrapperOneImpl)
{
    for (int i = 0; i < Size; i++)
    {
        data[i] = i;
    }

    ASSERT_TRUE(sut);
    EXPECT_EQ(sut->size(), Size);

    int num = 0;
    for (auto it = sut->cbegin(); it != sut->cend(); ++it)
    {
        EXPECT_EQ(*it, num++);
    }

    sut->consume(5, Dir::Backward);
    sut->consume(3, Dir::Forward);
    EXPECT_EQ(sut->size(), 24);

    num = 5;
    for (auto it = sut->cbegin(); it != sut->cend(); ++it)
    {
        EXPECT_EQ(*it, num++);
    }

    auto result = sut->allocate(5);
    EXPECT_EQ(sut->size(), 29);

    std::ranges::iota(result, 0);
    EXPECT_THAT(result, ElementsAre(0, 1, 2, 3, 4));
}


TEST_F(SocketBufferTest, NullBufferIsFalsy)
{
    SocketBuffer null_buffer(nullptr);
    EXPECT_FALSE(null_buffer);
    EXPECT_EQ(null_buffer.operator->(), nullptr);

    null_buffer.reset();
    EXPECT_FALSE(null_buffer);

    SocketBuffer empty;
    EXPECT_FALSE(empty);
}

TEST_F(SocketBufferTest, ResetReleasesAndRecycles)
{
    EXPECT_TRUE(sut);
    EXPECT_FALSE(called);

    sut.reset();

    EXPECT_FALSE(sut);
    EXPECT_TRUE(called);
}

TEST_F(SocketBufferTest, DereferenceExposesInterface)
{
    SocketBufferInterface& ref = *sut;

    EXPECT_EQ(&ref, sut.operator->());
    EXPECT_EQ(ref.size(), Size);
    EXPECT_EQ(ref.get_header_payload().data(), data);
    EXPECT_EQ(ref.get_header_payload().size(), Size);
}

TEST_F(SocketBufferTest, SequenceIsStoredAndCopiedByClone)
{
    EXPECT_EQ(sut->get_sequence(), 0);

    sut->set_sequence(7);
    EXPECT_EQ(sut->get_sequence(), 7);

    SocketBuffer cloned = sut.clone();

    EXPECT_TRUE(cloned);
    EXPECT_EQ(cloned->get_sequence(), 7);
    EXPECT_EQ(cloned->size(), Size);
}

TEST_F(SocketBufferTest, CopySharesStorageWithoutSharingPosition)
{
    for (int i = 0; i < Size; i++)
    {
        data[i] = static_cast<uint8_t>(i);
    }

    SocketBuffer copy = sut;

    EXPECT_TRUE(copy);
    EXPECT_EQ(copy->size(), Size);
    EXPECT_EQ(copy->get_header_payload().data(), sut->get_header_payload().data());

    EXPECT_EQ(sut->consume(4, Dir::Backward), 4u);
    EXPECT_EQ(sut->size(), Size - 4);
    EXPECT_EQ(copy->size(), Size);
    EXPECT_EQ(buffer_bytes(*sut).front(), 4);
    EXPECT_EQ(buffer_bytes(*copy).front(), 0);

    // The clone observes the very same bytes.
    data[0] = 0xAB;
    EXPECT_EQ(buffer_bytes(*copy).front(), 0xAB);
}

TEST_F(SocketBufferTest, MoveTransfersOwnership)
{
    SocketBuffer moved = std::move(sut);

    EXPECT_TRUE(moved);
    EXPECT_FALSE(sut);
    EXPECT_EQ(moved->size(), Size);
}

TEST_F(SocketBufferTest, AssignClonesSource)
{
    SocketBuffer other;
    EXPECT_FALSE(other);

    other = sut;

    EXPECT_TRUE(other);
    EXPECT_EQ(other->size(), Size);
    EXPECT_NE(other.operator->(), sut.operator->());

    sut.reset();
    EXPECT_EQ(other->size(), Size);
}

TEST_F(SocketBufferTest, IterateAndPostIncrement)
{
    for (int i = 0; i < Size; i++)
    {
        data[i] = static_cast<uint8_t>(i);
    }

    auto it = sut->begin();
    ASSERT_NE(it, sut->end());
    EXPECT_EQ(*it, 0);
    EXPECT_EQ(it.operator->(), data);

    const auto previous = it++;
    EXPECT_EQ(*previous, 0);
    EXPECT_EQ(*it, 1);

    int expected = 1;
    for (; it != sut->end(); ++it)
    {
        EXPECT_EQ(*it, expected++);
    }
    EXPECT_EQ(expected, Size);
}

TEST_F(SocketBufferTest, EmptyBufferBeginEqualsEnd)
{
    EXPECT_EQ(sut->consume(Size, Dir::Backward), static_cast<size_t>(Size));
    EXPECT_EQ(sut->size(), 0);
    EXPECT_EQ(sut->begin(), sut->end());
    EXPECT_EQ(sut->cbegin(), sut->cend());
}

TEST_F(SocketBufferTest, ConsumeForwardKeepsFrontPayload)
{
    for (int i = 0; i < Size; i++)
    {
        data[i] = static_cast<uint8_t>(i);
    }

    EXPECT_EQ(sut->consume(0, Dir::Forward), 0u);
    EXPECT_EQ(sut->consume(6, Dir::Forward), 6u);
    EXPECT_EQ(sut->size(), Size - 6);

    EXPECT_EQ(sut->get_header_payload().data(), data);
    EXPECT_EQ(sut->get_header_payload().size(), Size - 6);
    EXPECT_EQ(*sut->cbegin(), 0);
    EXPECT_EQ(buffer_bytes(*sut).back(), Size - 7);

    // Over consuming is clamped to the remaining payload.
    EXPECT_EQ(sut->consume(100, Dir::Forward), static_cast<size_t>(Size - 6));
    EXPECT_EQ(sut->size(), 0);
}

TEST_F(SocketBufferTest, ConsumeBackwardClampsToSize)
{
    EXPECT_EQ(sut->consume(1000, Dir::Backward), static_cast<size_t>(Size));
    EXPECT_EQ(sut->size(), 0);
    EXPECT_EQ(sut->consume(1), 0u);
}

TEST_F(SocketBufferTest, AllocatePrependsRecycledHeadroom)
{
    for (int i = 0; i < Size; i++)
    {
        data[i] = static_cast<uint8_t>(i);
    }

    // A fresh buffer fully covers the storage, so nothing can be prepended.
    EXPECT_TRUE(sut->allocate(1).empty());
    EXPECT_EQ(sut->size(), Size);

    sut->consume(10, Dir::Backward);
    ASSERT_EQ(sut->size(), Size - 10);

    auto head = sut->allocate(6);
    EXPECT_EQ(head.data(), data + 4);
    EXPECT_EQ(head.size(), 6u);
    EXPECT_EQ(sut->size(), Size - 4);
    EXPECT_EQ(sut->get_header_payload().data(), data + 4);

    head[0] = 0xAA;
    EXPECT_EQ(*sut->cbegin(), 0xAA);
    EXPECT_EQ(buffer_bytes(*sut).size(), static_cast<size_t>(Size - 4));
}

TEST_F(SocketBufferTest, AllocateFailsWhenHeadroomTooSmall)
{
    sut->consume(4, Dir::Backward);

    EXPECT_TRUE(sut->allocate(5).empty());
    EXPECT_EQ(sut->size(), Size - 4);
    EXPECT_EQ(sut->get_header_payload().data(), data + 4);

    EXPECT_EQ(sut->allocate(4).size(), 4u);
    EXPECT_EQ(sut->size(), Size);
    EXPECT_EQ(sut->get_header_payload().data(), data);

    EXPECT_TRUE(sut->allocate(4).empty());
}

TEST_F(SocketBufferTest, ReclaimResetsToTail)
{
    for (int i = 0; i < Size; i++)
    {
        data[i] = static_cast<uint8_t>(i);
    }

    sut->consume(12, Dir::Backward);
    sut->reclaim();

    EXPECT_EQ(sut->size(), 0);
    EXPECT_EQ(sut->cbegin(), sut->cend());
    EXPECT_EQ(sut->get_header_payload().data(), data + Size);
    EXPECT_EQ(sut->get_header_payload().size(), 0);

    auto tail = sut->allocate(4);
    EXPECT_EQ(tail.data(), data + Size - 4);
    EXPECT_EQ(tail.size(), 4u);
    EXPECT_EQ(sut->size(), 4u);
    EXPECT_EQ(buffer_bytes(*sut).size(), 4u);
}


TEST(SocketBufferImplStandaloneTest, ConstructWithWholeStorageKeepsPayload)
{
    uint8_t data[16];
    for (int i = 0; i < 16; i++)
    {
        data[i] = static_cast<uint8_t>(i);
    }

    int recycle_count = 0;
    const void* recycled = nullptr;

    auto buffer = SocketBuffer(std::make_shared<SocketBufferImpl>(data, 16, 16, [&](const void* ptr) {
        ++recycle_count;
        recycled = ptr;
    }));

    EXPECT_EQ(buffer->size(), 16);
    EXPECT_EQ(buffer->get_header_payload().data(), data);
    EXPECT_EQ(buffer_bytes(*buffer), sequential_bytes(16));
    EXPECT_EQ(recycle_count, 0);

    buffer.reset();

    EXPECT_EQ(recycle_count, 1);
    EXPECT_EQ(recycled, static_cast<const void*>(data));
}

TEST(SocketBufferImplStandaloneTest, ConstructWithoutLengthStartsEmptyAtTail)
{
    uint8_t data[16] = {};

    auto buffer = SocketBuffer(std::make_shared<SocketBufferImpl>(data, 16, std::nullopt));

    EXPECT_EQ(buffer->size(), 0);
    EXPECT_EQ(buffer->cbegin(), buffer->cend());
    EXPECT_EQ(buffer->get_header_payload().data(), data + 16);
    EXPECT_TRUE(buffer->get_header_payload().empty());
    EXPECT_EQ(buffer->consume(1), 0u);

    auto tail = buffer->allocate(3);
    EXPECT_EQ(tail.data(), data + 13);
    EXPECT_EQ(tail.size(), 3u);

    tail[0] = 0x01;
    tail[1] = 0x02;
    tail[2] = 0x03;

    const std::vector<uint8_t> expected{0x01, 0x02, 0x03};
    EXPECT_EQ(buffer->size(), 3u);
    EXPECT_EQ(buffer_bytes(*buffer), expected);
}

TEST(SocketBufferImplStandaloneTest, PartialLengthAllocatesAfterFullConsume)
{
    uint8_t data[16] = {};
    for (int i = 0; i < 10; i++)
    {
        data[i] = static_cast<uint8_t>(i);
    }

    auto buffer = SocketBuffer(std::make_shared<SocketBufferImpl>(data, 16, 10));

    EXPECT_EQ(buffer->size(), 10);
    EXPECT_EQ(buffer->get_header_payload().data(), data);
    EXPECT_EQ(buffer_bytes(*buffer), sequential_bytes(10));

    // The payload starts at the beginning of the storage, no headroom yet.
    EXPECT_TRUE(buffer->allocate(1).empty());

    EXPECT_EQ(buffer->consume(10, Dir::Backward), 10u);
    EXPECT_EQ(buffer->size(), 0);

    auto head = buffer->allocate(6);
    EXPECT_EQ(head.data(), data + 4);
    EXPECT_EQ(head.size(), 6u);
    EXPECT_EQ(buffer->size(), 6u);
}

TEST(SocketBufferImplStandaloneTest, CloneSharesStorageButKeepsOwnPayload)
{
    uint8_t data[8] = {0, 1, 2, 3, 4, 5, 6, 7};
    int recycle_count = 0; {
        auto buffer = SocketBuffer(std::make_shared<SocketBufferImpl>(data, 8, 8, [&](const void*) {
            ++recycle_count;
        }));
        buffer->set_sequence(3);

        auto clone = buffer->clone();

        ASSERT_TRUE(clone);
        EXPECT_NE(clone.get(), buffer.operator->());
        EXPECT_EQ(clone->get_sequence(), 3);
        EXPECT_EQ(clone->size(), 8u);
        EXPECT_EQ(clone->get_header_payload().data(), data);

        EXPECT_EQ(buffer->consume(2, Dir::Backward), 2u);
        EXPECT_EQ(buffer->size(), 6u);
        EXPECT_EQ(clone->size(), 8u);
        EXPECT_EQ(recycle_count, 0);

        clone.reset();
        EXPECT_EQ(recycle_count, 0);
    }

    // The storage is released only once the last view is gone.
    EXPECT_EQ(recycle_count, 1);
}

TEST(SocketBufferImplStandaloneTest, RecycleRunsOnceForSharedStorage)
{
    uint8_t data[8] = {};
    int recycle_count = 0; {
        auto buffer = SocketBuffer(std::make_shared<SocketBufferImpl>(data, 8, 8, [&](const void*) {
            ++recycle_count;
        }));

        auto copy = SocketBuffer(buffer);
        auto deep = buffer.clone();

        EXPECT_EQ(buffer->size(), 8u);
        EXPECT_EQ(copy->size(), 8u);
        EXPECT_EQ(deep->size(), 8u);

        buffer.reset();
        EXPECT_EQ(recycle_count, 0);

        copy.reset();
        EXPECT_EQ(recycle_count, 0);

        deep.reset();
        EXPECT_EQ(recycle_count, 1);
    }

    EXPECT_EQ(recycle_count, 1);
}


TEST(BulkSocketBufferImplTest, EmptyBuffer)
{
    BulkSocketBufferImpl bulk;

    EXPECT_EQ(bulk.size(), 0u);
    EXPECT_EQ(bulk.get_sequence(), 0);
    EXPECT_TRUE(bulk.get_header_payload().empty());
    EXPECT_EQ(bulk.cbegin(), bulk.cend());
    EXPECT_EQ(bulk.consume(4), 0u);
    EXPECT_EQ(bulk.consume(4, Dir::Forward), 0u);
    EXPECT_TRUE(bulk.allocate(4).empty());
}

TEST(BulkSocketBufferImplTest, AddKeepsChildrenSortedBySequence)
{
    uint8_t third[4];
    uint8_t first[2];
    uint8_t second[5];
    std::fill_n(third, 4, 0x33);
    std::fill_n(first, 2, 0x11);
    std::fill_n(second, 5, 0x22);

    BulkSocketBufferImpl bulk;
    bulk.add(make_socket_buffer(third, 4, 30));
    bulk.add(make_socket_buffer(second, 5, 20));
    bulk.add(make_socket_buffer(first, 2, 10));

    EXPECT_EQ(bulk.size(), 11u);
    EXPECT_EQ(bulk.get_header_payload().data(), first);
    EXPECT_EQ(bulk.get_header_payload().size(), 2u);

    const std::vector<uint8_t> expected{0x11, 0x11, 0x22, 0x22, 0x22, 0x22, 0x22, 0x33, 0x33, 0x33, 0x33};
    EXPECT_EQ(buffer_bytes(bulk), expected);
}

TEST(BulkSocketBufferImplTest, ConsumeBackwardSpansChildren)
{
    uint8_t front[4];
    uint8_t back[4];
    std::fill_n(front, 4, 0x11);
    std::fill_n(back, 4, 0x22);

    BulkSocketBufferImpl bulk;
    bulk.add(make_socket_buffer(front, 4, 1));
    bulk.add(make_socket_buffer(back, 4, 2));

    EXPECT_EQ(bulk.consume(6, Dir::Backward), 6u);
    EXPECT_EQ(bulk.size(), 2u);

    const std::vector<uint8_t> expected{0x22, 0x22};
    EXPECT_EQ(buffer_bytes(bulk), expected);

    // The drained child is skipped while consuming the remainder.
    EXPECT_EQ(bulk.consume(3, Dir::Backward), 2u);
    EXPECT_EQ(bulk.size(), 0u);
}

TEST(BulkSocketBufferImplTest, ConsumeForwardSpansChildren)
{
    uint8_t front[4];
    uint8_t back[4];
    std::fill_n(front, 4, 0x11);
    std::fill_n(back, 4, 0x22);

    BulkSocketBufferImpl bulk;
    bulk.add(make_socket_buffer(front, 4, 1));
    bulk.add(make_socket_buffer(back, 4, 2));

    EXPECT_EQ(bulk.consume(6, Dir::Forward), 6u);
    EXPECT_EQ(bulk.size(), 2u);

    const std::vector<uint8_t> expected{0x11, 0x11};
    EXPECT_EQ(buffer_bytes(bulk), expected);
}

TEST(BulkSocketBufferImplTest, ConsumeMoreThanAvailableIsClamped)
{
    uint8_t front[4];
    uint8_t back[4];
    std::fill_n(front, 4, 0x11);
    std::fill_n(back, 4, 0x22);

    BulkSocketBufferImpl bulk;
    bulk.add(make_socket_buffer(front, 4, 1));
    bulk.add(make_socket_buffer(back, 4, 2));

    EXPECT_EQ(bulk.consume(0), 0u);
    EXPECT_EQ(bulk.size(), 8u);

    EXPECT_EQ(bulk.consume(100, Dir::Backward), 8u);
    EXPECT_EQ(bulk.size(), 0u);
    EXPECT_EQ(bulk.consume(1, Dir::Backward), 0u);
    EXPECT_EQ(bulk.consume(1, Dir::Forward), 0u);
}

TEST(BulkSocketBufferImplTest, ReclaimDropsAllChildren)
{
    uint8_t front[4];
    uint8_t back[4];
    std::fill_n(front, 4, 0x11);
    std::fill_n(back, 4, 0x22);

    BulkSocketBufferImpl bulk;
    bulk.add(make_socket_buffer(front, 4, 1));
    bulk.add(make_socket_buffer(back, 4, 2));
    ASSERT_EQ(bulk.size(), 8u);

    bulk.reclaim();

    EXPECT_EQ(bulk.size(), 0u);
    EXPECT_EQ(bulk.cbegin(), bulk.cend());
    EXPECT_TRUE(bulk.get_header_payload().empty());
    EXPECT_EQ(bulk.consume(1), 0u);
}

TEST(BulkSocketBufferImplTest, CloneCopiesChildrenAndSequence)
{
    uint8_t front[4];
    uint8_t back[4];
    std::fill_n(front, 4, 0x11);
    std::fill_n(back, 4, 0x22);

    BulkSocketBufferImpl bulk;
    bulk.add(make_socket_buffer(front, 4, 1));
    bulk.add(make_socket_buffer(back, 4, 2));
    bulk.set_sequence(9);

    auto clone = bulk.clone();

    ASSERT_TRUE(clone);
    EXPECT_NE(clone.get(), static_cast<SocketBufferInterface*>(&bulk));
    EXPECT_EQ(clone->get_sequence(), 9);
    EXPECT_EQ(clone->size(), 8u);
    EXPECT_EQ(buffer_bytes(*clone), buffer_bytes(bulk));

    clone->consume(4, Dir::Backward);

    EXPECT_EQ(clone->size(), 4u);
    EXPECT_EQ(bulk.size(), 8u);
}

TEST(BulkSocketBufferImplTest, WrapperDelegatesToBulk)
{
    uint8_t front[4];
    uint8_t back[4];
    std::fill_n(front, 4, 0x11);
    std::fill_n(back, 4, 0x22);

    auto bulk = std::make_shared<BulkSocketBufferImpl>();
    bulk->add(make_socket_buffer(front, 4, 1));
    bulk->add(make_socket_buffer(back, 4, 2));

    SocketBuffer wrapped(bulk);

    ASSERT_TRUE(wrapped);
    EXPECT_EQ(wrapped->size(), 8u);

    const std::vector<uint8_t> expected{0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x22, 0x22};
    EXPECT_EQ(buffer_bytes(*wrapped), expected);

    SocketBuffer cloned = wrapped.clone();
    EXPECT_EQ(buffer_bytes(*cloned), expected);

    EXPECT_EQ(wrapped->consume(8, Dir::Backward), 8u);
    EXPECT_EQ(cloned->size(), 8u);
}

TEST(SocketBufferIteratorTest, DefaultIteratorHasNoOwner)
{
    const SocketBufferIterator<uint8_t> it;

    EXPECT_EQ(it, SocketBufferIterator<uint8_t>{});
}

TEST(SocketBufferIteratorTest, ViewWithoutPayloadEqualsEnd)
{
    FakeIteratorView view;
    view.append(std::span<uint8_t>{});
    view.append(std::span<uint8_t>{});

    EXPECT_EQ(view.get_span_count(), 2u);
    EXPECT_EQ(SocketBufferIterator<uint8_t>(&view), SocketBufferIterator<uint8_t>{});
}

TEST(SocketBufferIteratorTest, SkipsEmptySpansWhileIterating)
{
    uint8_t head[2] = {1, 2};
    uint8_t tail[3] = {3, 4, 5};

    FakeIteratorView view;
    view.append(std::span(head));
    view.append(std::span<uint8_t>{});
    view.append(std::span(tail));
    view.append(std::span<uint8_t>{});

    std::vector<uint8_t> visited;
    for (auto it = SocketBufferIterator<uint8_t>(&view); it != SocketBufferIterator<uint8_t>{}; ++it)
    {
        visited.push_back(*it);
    }

    const std::vector<uint8_t> expected{1, 2, 3, 4, 5};
    EXPECT_EQ(visited, expected);
}

TEST(SocketBufferIteratorTest, DereferenceAndPostIncrement)
{
    uint8_t bytes[3] = {10, 20, 30};

    FakeIteratorView view;
    view.append(std::span(bytes));

    auto it = SocketBufferIterator<uint8_t>(&view);

    EXPECT_EQ(*it, 10);
    EXPECT_EQ(it.operator->(), bytes);
    EXPECT_EQ(*it.operator->(), 10);

    *it = static_cast<uint8_t>(0x42);
    EXPECT_EQ(bytes[0], 0x42);

    const auto previous = it++;
    EXPECT_EQ(*previous, 0x42);
    EXPECT_EQ(*it, 20);

    ++it;
    EXPECT_EQ(*it, 30);
    ++it;
    EXPECT_EQ(it, SocketBufferIterator<uint8_t>{});
}

TEST(SocketBufferIteratorTest, ConstIteratorIsReadOnly)
{
    static_assert(std::is_same_v<std::iterator_traits<SocketBufferIterator<uint8_t>>::value_type, uint8_t>);
    static_assert(std::is_same_v<std::iterator_traits<SocketBufferIterator<const uint8_t>>::value_type, const uint8_t>);
    static_assert(std::is_same_v<std::iterator_traits<SocketBufferIterator<const uint8_t>>::iterator_category,
        std::bidirectional_iterator_tag>);
    static_assert(std::is_same_v<decltype(*std::declval<const SocketBufferIterator<const uint8_t>&>()), const uint8_t&>);

    uint8_t bytes[2] = {7, 8};

    FakeIteratorView view;
    view.append(std::span(bytes));

    auto it = SocketBufferIterator<const uint8_t>(&view);

    EXPECT_EQ(*it, 7);
    EXPECT_EQ(it.operator->(), bytes);

    ++it;
    EXPECT_EQ(*it, 8);
    ++it;
    EXPECT_EQ(it, SocketBufferIterator<const uint8_t>{});
}

}
