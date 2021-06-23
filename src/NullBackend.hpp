/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_NULL_BACKEND_HPP
#define __SONATA_NULL_BACKEND_HPP
#include "sonata/Admin.hpp"
#include "sonata/Backend.hpp"
#include "sonata/Client.hpp"

#include <cstdio>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <thallium.hpp>

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;
using nlohmann::json;

class NullBackend : public Backend {

public:
  NullBackend(const tl::engine& engine,
              uint64_t delay_ms=0,
              bool active_delay=false,
              bool lock_mutex=false)
  : m_engine(engine),
    m_delay_ms(delay_ms),
    m_active_delay(active_delay),
    m_lock_mutex(lock_mutex) {}

  NullBackend(NullBackend &&) = delete;

  NullBackend(const NullBackend &) = delete;

  NullBackend &operator=(NullBackend &&) = delete;

  NullBackend &operator=(const NullBackend &) = delete;

  static std::unique_ptr<Backend> create(const tl::engine &engine,
                                         const tl::pool &pool,
                                         const json &config);

  static std::unique_ptr<Backend> attach(const tl::engine &engine,
                                         const tl::pool &pool,
                                         const json &config);

  virtual ~NullBackend() {}

  virtual RequestResult<bool>
  createCollection(const std::string &coll_name) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<bool> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<bool>
  openCollection(const std::string &coll_name) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<bool> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<bool>
  dropCollection(const std::string &coll_name) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<bool> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<uint64_t> store(const std::string &coll_name,
                                        const std::string &record,
                                        bool commit) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<uint64_t> result;
    result.value() = 0;
    result.success() = true;
    return result;
  }

  virtual RequestResult<uint64_t> storeJson(const std::string &coll_name,
                                            const JsonWrapper &record,
                                            bool commit) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<uint64_t> result;
    result.value() = 0;
    result.success() = true;
    return result;
  }

  virtual RequestResult<std::vector<uint64_t>>
  storeMulti(const std::string &coll_name,
             const std::vector<std::string> &records, bool commit) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<std::vector<uint64_t>> result;
    result.value().resize(records.size());
    for (unsigned i = 0; i < records.size(); i++)
      result.value()[i] = i;
    result.success() = true;
    return result;
  }

  virtual RequestResult<bool> commit() override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<bool> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<std::vector<uint64_t>>
  storeMultiJson(const std::string &coll_name, const JsonWrapper &records,
                 bool commit) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<std::vector<uint64_t>> result;
    result.value().resize(records->size());
    for (unsigned i = 0; i < records->size(); i++)
      result.value()[i] = i;
    result.success() = true;
    return result;
  }

  virtual RequestResult<std::string> fetch(const std::string &coll_name,
                                           uint64_t record_id) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<std::string> result;
    result.value() = "{}";
    result.success() = true;
    return result;
  }

  virtual RequestResult<JsonWrapper> fetchJson(const std::string &coll_name,
                                               uint64_t record_id) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<JsonWrapper> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<std::vector<std::string>>
  fetchMulti(const std::string &coll_name,
             const std::vector<uint64_t> &record_ids) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<std::vector<std::string>> result;
    result.value().resize(record_ids.size(), "{}");
    result.success() = true;
    return result;
  }

  virtual RequestResult<JsonWrapper>
  fetchMultiJson(const std::string &coll_name,
                 const std::vector<uint64_t> &record_ids) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<JsonWrapper> result;
    result.value() = json::array();
    for(size_t i = 0; i < record_ids.size(); i++)
        result.value()->emplace_back();
    result.success() = true;
    return result;
  }

  virtual RequestResult<std::vector<std::string>>
  filter(const std::string &coll_name,
         const std::string &filter_code) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<std::vector<std::string>> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<JsonWrapper>
  filterJson(const std::string &coll_name,
             const std::string &filter_code) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<JsonWrapper> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<bool> update(const std::string &coll_name,
                                     uint64_t record_id,
                                     const std::string &new_content,
                                     bool commit) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<bool> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<bool> updateJson(const std::string &coll_name,
                                         uint64_t record_id,
                                         const JsonWrapper &new_content,
                                         bool commit) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<bool> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<std::vector<bool>> updateMulti(
      const std::string &coll_name, const std::vector<uint64_t> &record_ids,
      const std::vector<std::string> &new_contents, bool commit) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<std::vector<bool>> result;
    result.value().resize(record_ids.size(), true);
    result.success() = true;
    return result;
  }

  virtual RequestResult<std::vector<bool>>
  updateMultiJson(const std::string &coll_name,
                  const std::vector<uint64_t> &record_ids,
                  const JsonWrapper &new_contents, bool commit) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<std::vector<bool>> result;
    result.value().resize(record_ids.size(), true);
    result.success() = true;
    return result;
  }

  virtual RequestResult<std::vector<std::string>>
  all(const std::string &coll_name) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<std::vector<std::string>> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<JsonWrapper>
  allJson(const std::string &coll_name) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<JsonWrapper> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<uint64_t>
  lastID(const std::string &coll_name) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<uint64_t> result;
    result.value() = 0;
    result.success() = true;
    return result;
  }

  virtual RequestResult<size_t> size(const std::string &coll_name) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<size_t> result;
    result.value() = 0;
    result.success() = true;
    return result;
  }

  virtual RequestResult<bool> erase(const std::string &coll_name,
                                    uint64_t record_id, bool commit) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<bool> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<bool>
  eraseMulti(const std::string &coll_name,
             const std::vector<uint64_t> &record_ids, bool commit) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<bool> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<std::unordered_map<std::string, std::string>>
  execute(const std::string &code, const std::unordered_set<std::string> &vars,
          bool commit) override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<std::unordered_map<std::string, std::string>> result;
    for (const auto &v : vars) {
      result.value().emplace(v, "null");
    }
    result.success() = true;
    return result;
  }

  virtual RequestResult<bool> destroy() override {
    std::unique_lock<tl::mutex> lock;
    if (m_lock_mutex)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    if (m_delay_ms > 0) {
        if (m_active_delay) {
            double start = tl::timer::wtime()*1000;
            double end = start;
            while(end - start < m_delay_ms)
                end = tl::timer::wtime()*1000;
        } else {
            tl::thread::sleep(m_engine, m_delay_ms);
        }
    }
    RequestResult<bool> result;
    result.success() = true;
    return result;
  }

  std::string getConfig() const override {
      std::stringstream ss;
      ss << "{\"delay_ms\":" << m_delay_ms
         << ",\"active_delay\":" << std::boolalpha << m_active_delay
         << ",\"lock_mutex\":" << std::boolalpha << m_lock_mutex
         << "}";
      return ss.str();
  }

private:
  tl::engine m_engine;
  uint64_t   m_delay_ms = 0;
  bool       m_active_delay = false;
  bool       m_lock_mutex = false;
  tl::mutex  m_mutex;
};

} // namespace sonata
#endif
