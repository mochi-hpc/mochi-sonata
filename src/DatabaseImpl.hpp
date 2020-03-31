#ifndef __SONATA_DATABASE_IMPL_H
#define __SONATA_DATABASE_IMPL_H

namespace sonata {

namespace tl = thallium;

class DatabaseImpl {

    public:

    std::string                 m_name;
    std::shared_ptr<ClientImpl> m_client;
    tl::provider_handle         m_ph;
    Json::CharReader*           m_json_reader = nullptr;

    DatabaseImpl() = default;

    DatabaseImpl(const std::shared_ptr<ClientImpl>& client,
                 tl::provider_handle&& ph, const std::string& name)
    : m_name(name)
    , m_client(client)
    , m_ph(std::move(ph)) {
        Json::CharReaderBuilder builder;
        m_json_reader = builder.newCharReader();
    }

    ~DatabaseImpl() {
        delete m_json_reader;
    }

};

}

#endif
