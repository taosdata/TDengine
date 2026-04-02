PROJECT(TDengine)

# ADD_SUBDIRECTORY(admin)
# ADD_SUBDIRECTORY(account)

# Add taosk module (now uses community CBC encryption, no OpenSSL required)
ADD_SUBDIRECTORY(grant)
# ADD_SUBDIRECTORY(tsdb)
ADD_SUBDIRECTORY(storage)
# ADD_SUBDIRECTORY(module)
ADD_SUBDIRECTORY(dnode)
# ADD_SUBDIRECTORY(web)