MAGIC_FLAGS_CPPFLAGS := -I/home/josh/hypertable/build/0.9.0.10/include
vpath %.a /home/josh/hypertable/build/0.9.0.10/lib
vpath %.so /home/josh/hypertable/build/0.9.0.10/lib

MAGIC_MODULE_DEPS := kdi
MAGIC_EXTERNAL_DEPS := Hypertable
MAGIC_LINK_TYPE := shared
include magic.mk
