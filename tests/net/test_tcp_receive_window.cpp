#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <common/socket_buffer.h>
#include <protocol/manager/transport/tcp_receive_window.h>
#include <cstdint>
#include <list>
#include <memory>
#include <vector>

using namespace ::testing;
using namespace coro::net;
using namespace coro::net::trans;

namespace coro::ut {

class TcpReceiveWindowTest : public ::testing::Test {
protected:
    static constexpr uint32_t WindowSize = 8192;
    static constexpr uint32_t InitialSequence = 1000;

    void SetUp() override
    {
        sut = std::make_unique<TcpReceiveWindow>(logger(), WindowSize);
        sut->set_next_expected_sequence(InitialSequence);
    }

    static std::shared_ptr<spdlog::logger> logger()
    {
        static auto instance = [] {
            auto created = std::make_shared<spdlog::logger>("ut.tcp_receive_window");
            created->set_level(spdlog::level::off);
            return created;
        }();

        return instance;
    }

    SocketBuffer segment(uint32_t sequence, uint32_t length)
    {
        auto& bytes = storage.emplace_back(length);
        for (uint32_t i = 0; i < length; i++)
        {
            bytes[i] = static_cast<uint8_t>((sequence + i) & 0xff);
        }

        return SocketBuffer(SocketBufferImpl(bytes.data(), bytes.size(), bytes.size()));
    }

    static std::vector<uint8_t> expected(uint32_t sequence, uint32_t length)
    {
        std::vector<uint8_t> bytes(length);
        for (uint32_t i = 0; i < length; i++)
        {
            bytes[i] = static_cast<uint8_t>((sequence + i) & 0xff);
        }

        return bytes;
    }

    static std::vector<uint8_t> read(TcpReceiveWindow& window, size_t size)
    {
        std::vector<uint8_t> out(size);
        out.resize(window.read_into(out));
        return out;
    }

    std::list<std::vector<uint8_t>> storage;
    std::unique_ptr<TcpReceiveWindow> sut;
};

TEST_F(TcpReceiveWindowTest, InOrderSegmentIsReadBack)
{
    EXPECT_TRUE(sut->reassemble(1000, segment(1000, 100)));
    EXPECT_EQ(sut->get_next_expected_sequence(), 1100u);
    EXPECT_EQ(sut->already_used_bytes(), 100u);
    EXPECT_EQ(sut->available_window(), static_cast<uint16_t>(WindowSize - 100));

    EXPECT_EQ(read(*sut, 100), expected(1000, 100));
    EXPECT_EQ(sut->already_used_bytes(), 0u);
    EXPECT_EQ(sut->available_window(), static_cast<uint16_t>(WindowSize));
}

TEST_F(TcpReceiveWindowTest, OutOfOrderSegmentsAdvanceAfterGapFilled)
{
    EXPECT_FALSE(sut->reassemble(1200, segment(1200, 100)));
    EXPECT_FALSE(sut->reassemble(1300, segment(1300, 100)));
    EXPECT_EQ(sut->get_next_expected_sequence(), 1000u);
    EXPECT_EQ(sut->already_used_bytes(), 200u);

    EXPECT_TRUE(sut->reassemble(1000, segment(1000, 200)));
    EXPECT_EQ(sut->get_next_expected_sequence(), 1400u);
    EXPECT_EQ(sut->already_used_bytes(), 400u);

    EXPECT_EQ(read(*sut, 400), expected(1000, 400));
    EXPECT_EQ(sut->already_used_bytes(), 0u);
}

TEST_F(TcpReceiveWindowTest, DuplicateOutOfOrderSegmentIsNotCountedTwice)
{
    EXPECT_FALSE(sut->reassemble(1200, segment(1200, 100)));
    EXPECT_FALSE(sut->reassemble(1200, segment(1200, 100)));
    EXPECT_EQ(sut->already_used_bytes(), 100u);
    EXPECT_EQ(sut->available_window(), static_cast<uint16_t>(WindowSize - 100));

    EXPECT_TRUE(sut->reassemble(1000, segment(1000, 200)));
    EXPECT_EQ(sut->already_used_bytes(), 300u);

    EXPECT_EQ(read(*sut, 300), expected(1000, 300));
    EXPECT_EQ(sut->already_used_bytes(), 0u);
    EXPECT_EQ(sut->available_window(), static_cast<uint16_t>(WindowSize));
}

TEST_F(TcpReceiveWindowTest, FullyAcknowledgedSegmentIsIgnored)
{
    EXPECT_TRUE(sut->reassemble(1000, segment(1000, 100)));

    EXPECT_FALSE(sut->reassemble(1000, segment(1000, 100)));
    EXPECT_FALSE(sut->reassemble(900, segment(900, 100)));
    EXPECT_EQ(sut->get_next_expected_sequence(), 1100u);
    EXPECT_EQ(sut->already_used_bytes(), 100u);
}

TEST_F(TcpReceiveWindowTest, PartiallyAcknowledgedSegmentKeepsNewTail)
{
    EXPECT_TRUE(sut->reassemble(1000, segment(1000, 100)));

    EXPECT_TRUE(sut->reassemble(500, segment(500, 700)));
    EXPECT_EQ(sut->get_next_expected_sequence(), 1200u);
    EXPECT_EQ(sut->already_used_bytes(), 200u);

    EXPECT_EQ(read(*sut, 200), expected(1000, 200));
}

TEST_F(TcpReceiveWindowTest, OverlappingRetransmissionKeepsPrefixOnly)
{
    EXPECT_FALSE(sut->reassemble(2000, segment(2000, 100)));
    EXPECT_TRUE(sut->reassemble(1000, segment(1000, 1500)));

    EXPECT_EQ(sut->get_next_expected_sequence(), 2100u);
    EXPECT_EQ(sut->already_used_bytes(), 1100u);

    EXPECT_EQ(read(*sut, 1100), expected(1000, 1100));
    EXPECT_EQ(sut->already_used_bytes(), 0u);
}

TEST_F(TcpReceiveWindowTest, SegmentSharingBufferedStartIsDropped)
{
    EXPECT_FALSE(sut->reassemble(1200, segment(1200, 100)));
    EXPECT_FALSE(sut->reassemble(1200, segment(1200, 300)));

    EXPECT_EQ(sut->get_next_expected_sequence(), 1000u);
    EXPECT_EQ(sut->already_used_bytes(), 100u);
}

TEST_F(TcpReceiveWindowTest, WindowFullClipsTailAndRejectsFurtherData)
{
    TcpReceiveWindow window(logger(), 1024);
    window.set_next_expected_sequence(1000);

    EXPECT_TRUE(window.reassemble(1000, segment(1000, 1000)));
    EXPECT_EQ(window.available_window(), 24);

    EXPECT_TRUE(window.reassemble(2000, segment(2000, 100)));
    EXPECT_EQ(window.available_window(), 0);
    EXPECT_EQ(window.get_next_expected_sequence(), 2024u);

    EXPECT_FALSE(window.reassemble(3000, segment(3000, 100)));
    EXPECT_EQ(window.get_next_expected_sequence(), 2024u);

    EXPECT_FALSE(window.need_window_update(100));

    EXPECT_EQ(read(window, 1024), expected(1000, 1024));
    EXPECT_EQ(window.available_window(), 1024);
}

TEST_F(TcpReceiveWindowTest, WindowUpdatePolicy)
{
    TcpReceiveWindow window(logger(), 1024);
    window.set_next_expected_sequence(1000);

    window.reassemble(1000, segment(1000, 1024));
    window.mark_window_advertised();
    EXPECT_EQ(window.available_window(), 0);
    EXPECT_FALSE(window.need_window_update(100));

    read(window, 1);
    EXPECT_TRUE(window.need_window_update(100));

    TcpReceiveWindow threshold(logger(), 1024);
    threshold.set_next_expected_sequence(1000);
    threshold.reassemble(1000, segment(1000, 100));
    threshold.mark_window_advertised();
    EXPECT_EQ(threshold.available_window(), 924);
    EXPECT_FALSE(threshold.need_window_update(100));

    read(threshold, 100);
    EXPECT_TRUE(threshold.need_window_update(100));
}

TEST_F(TcpReceiveWindowTest, PartialReadKeepsRemainder)
{
    EXPECT_TRUE(sut->reassemble(1000, segment(1000, 200)));

    EXPECT_EQ(read(*sut, 50), expected(1000, 50));
    EXPECT_EQ(sut->already_used_bytes(), 150u);

    EXPECT_EQ(read(*sut, 500), expected(1050, 150));
    EXPECT_EQ(sut->already_used_bytes(), 0u);
}

TEST_F(TcpReceiveWindowTest, ReadStopsAtGap)
{
    EXPECT_FALSE(sut->reassemble(1200, segment(1200, 100)));
    EXPECT_TRUE(sut->reassemble(1000, segment(1000, 100)));
    EXPECT_EQ(sut->get_next_expected_sequence(), 1100u);

    EXPECT_TRUE(sut->has_readable_data());
    EXPECT_EQ(read(*sut, 500), expected(1000, 100));
    EXPECT_EQ(sut->already_used_bytes(), 100u);
    EXPECT_FALSE(sut->has_readable_data());
}

TEST_F(TcpReceiveWindowTest, FinCompletesReceiveAfterDataRead)
{
    EXPECT_TRUE(sut->reassemble(1000, segment(1000, 100)));

    sut->acknowledge_finish_sequence(1100);
    EXPECT_EQ(sut->get_next_expected_sequence(), 1101u);
    EXPECT_TRUE(sut->is_receive_complete());
    EXPECT_FALSE(sut->is_eof());

    EXPECT_EQ(read(*sut, 200), expected(1000, 100));
    EXPECT_TRUE(sut->is_eof());
    EXPECT_TRUE(read(*sut, 10).empty());
}

TEST_F(TcpReceiveWindowTest, FinArrivingBeforeGapIsCompletedLater)
{
    EXPECT_TRUE(sut->reassemble(1000, segment(1000, 100)));
    sut->acknowledge_finish_sequence(1200);

    EXPECT_FALSE(sut->is_receive_complete());
    EXPECT_EQ(sut->get_next_expected_sequence(), 1100u);

    EXPECT_TRUE(sut->reassemble(1100, segment(1100, 100)));
    EXPECT_EQ(sut->get_next_expected_sequence(), 1201u);
    EXPECT_TRUE(sut->is_receive_complete());
    EXPECT_EQ(read(*sut, 200), expected(1000, 200));
    EXPECT_TRUE(sut->is_eof());
}

}
