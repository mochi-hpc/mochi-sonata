#ifndef __REQUEST_RESULT_HPP
#define __REQUEST_RESULT_HPP

namespace sonata {

template<typename T>
class RequestResult {
    
    public:

    RequestResult() = default;
    RequestResult(RequestResult&&) = default;
    RequestResult(const RequestResult&) = default;
    RequestResult& operator=(RequestResult&&) = default;
    RequestResult& operator=(const RequestResult&) = default;

    bool& success() {
        return m_success;
    }

    const bool& success() const {
        return m_success;
    }

    std::string& error() {
        return m_error;
    }

    const std::string& error() const {
        return m_error;
    }

    T& value() {
        return m_value;
    }

    const T& value() const {
        return m_value;
    }

    template<typename Archive>
    void serialize(Archive& a) {
        a & m_success;
        a & m_error;
        a & m_value;
    }

    private:

    bool        m_success;
    std::string m_error;
    T           m_value;
};

template<>
class RequestResult<std::string> {
    
    public:

    RequestResult() = default;
    RequestResult(RequestResult&&) = default;
    RequestResult(const RequestResult&) = default;
    RequestResult& operator=(RequestResult&&) = default;
    RequestResult& operator=(const RequestResult&) = default;

    bool& success() {
        return m_success;
    }

    const bool& success() const {
        return m_success;
    }

    std::string& error() {
        return m_content;
    }

    const std::string& error() const {
        return m_content;
    }

    std::string& value() {
        return m_content;
    }

    const std::string& value() const {
        return m_content;
    }

    template<typename Archive>
    void serialize(Archive& a) {
        a & m_success;
        a & m_content;
    }

    private:

    bool        m_success;
    std::string m_content;
};

template<>
class RequestResult<bool> {
    
    public:

    RequestResult() = default;
    RequestResult(RequestResult&&) = default;
    RequestResult(const RequestResult&) = default;
    RequestResult& operator=(RequestResult&&) = default;
    RequestResult& operator=(const RequestResult&) = default;

    bool& success() {
        return m_success;
    }

    const bool& success() const {
        return m_success;
    }

    std::string& error() {
        return m_error;
    }

    const std::string& error() const {
        return m_error;
    }

    bool& value() {
        return m_success;
    }

    const bool& value() const {
        return m_success;
    }

    template<typename Archive>
    void serialize(Archive& a) {
        a & m_success;
        a & m_error;
    }

    private:

    bool        m_success;
    std::string m_error;
};

}

#endif
