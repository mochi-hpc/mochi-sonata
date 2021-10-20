/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <cstdint>
#include <utility>

namespace sonata {

class Endian
{
  private:
    static constexpr uint32_t uint32_ = 0x01020304;
    static constexpr uint8_t magic_ = (const uint8_t&)uint32_;
  public:
    static constexpr bool little = magic_ == 0x04;
    static constexpr bool middle = magic_ == 0x02;
    static constexpr bool big = magic_ == 0x01;
    static_assert(little || middle || big, "Cannot determine endianness!");

    template<typename T>
    static T swap(T x) {
        uint8_t* str = (uint8_t*)&x;
        for(unsigned i=0; i < sizeof(x)/2; i++) {
            std::swap(str[i], str[sizeof(x)-1-i]);
        }
        return x;
    }

  private:
    Endian() = delete;
};

} // namespace sonata
