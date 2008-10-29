MAGIC_MODULE_DEPS := kdi_tablet kdi_meta
MAGIC_EXTERNAL_DEPS := Ice IceUtil
include magic.mk

# Manually add dependencies on generated headers
$(DEP_DIR)/TableManagerI.d $(BUILD_DIR)/TableManagerI.o: $(BUILD_DIR)/TableManager.h
$(DEP_DIR)/TimeoutLocator.d $(BUILD_DIR)/TimeoutLocator.o: $(BUILD_DIR)/TableManager.h
$(DEP_DIR)/net_table.d $(BUILD_DIR)/net_table.o: $(BUILD_DIR)/TableManager.h
