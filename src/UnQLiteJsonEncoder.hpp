/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_UNQLITE_JSON_ENCODE_HPP
#define __SONATA_UNQLITE_JSON_ENCODE_HPP

#include <nlohmann/json.hpp>
#include "Endian.hpp"

namespace sonata {

using nlohmann::json;

class UnQLiteJsonEncoder {

    static void do_encode(std::vector<char>& buffer, const json& obj) {
        auto type = obj.type();
        switch(type) {
        case json::value_t::null:
            buffer.push_back(23);
            break;
        case json::value_t::object:
            buffer.push_back(1);
            for(auto& p : obj.items()) {
                do_encode(buffer, p.key());
                buffer.push_back(5);
                do_encode(buffer, p.value());
                buffer.push_back(6);
            }
            buffer.push_back(2);
            break;
        case json::value_t::array:
            buffer.push_back(3);
            for(auto& e : obj) {
                do_encode(buffer, e);
                buffer.push_back(6);
            }
            buffer.push_back(4);
            break;
        case json::value_t::string:
            buffer.push_back(8);
            {
                auto& str = obj.get_ref<const std::string&>();
                uint32_t len = static_cast<uint32_t>(str.size());
                if(Endian::little) len = Endian::swap(len);
                buffer.resize(buffer.size()+sizeof(len));
                std::memcpy(buffer.data()+buffer.size()-sizeof(len), &len, sizeof(len));
                buffer.resize(buffer.size()+str.size());
                std::memcpy(buffer.data()+buffer.size()-str.size(), str.data(), str.size());
            }
            break;
        case json::value_t::boolean:
            if(obj.get<bool>()) buffer.push_back(24);
            else buffer.push_back(25);
            break;
        case json::value_t::number_integer:
        case json::value_t::number_unsigned:
            buffer.push_back(10);
            {
                int64_t val = obj.get<int64_t>();
                if(Endian::little) val = Endian::swap(val);
                buffer.resize(buffer.size()+sizeof(val));
                std::memcpy(buffer.data()+buffer.size()-sizeof(val), &val, sizeof(val));
            }
            break;
        case json::value_t::number_float:
            buffer.push_back(18);
            {
                auto str_val = std::to_string(obj.get<double>());
                uint16_t str_val_size = static_cast<uint16_t>(str_val.size());
                if(Endian::little) str_val_size = Endian::swap(str_val_size);
                buffer.resize(buffer.size()+sizeof(str_val_size));
                std::memcpy(buffer.data()+buffer.size()-sizeof(str_val_size), &str_val_size, sizeof(str_val_size));
                buffer.resize(buffer.size()+str_val.size());
                std::memcpy(buffer.data()+buffer.size()-str_val.size(), str_val.data(), str_val.size());
            }
            break;
        case json::value_t::binary:
        case json::value_t::discarded:
            break;
        }
    }

    public:

    static std::vector<char> encode(const json& obj) {
        std::vector<char> result;
        do_encode(result, obj);
        return result;
    }

};

} // namespace sonata

#endif
