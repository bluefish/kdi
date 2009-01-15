MAGIC_MODULE_DEPS := kdi_tablet kdi_meta
MAGIC_EXTERNAL_DEPS := Ice IceUtil
include magic.mk

# Manually add dependencies on generated headers
$(DEP_DIR)/TableManagerI.d $(BUILD_DIR)/TableManagerI.o: $(BUILD_DIR)/TableManager.h
$(DEP_DIR)/TableLocator.d $(BUILD_DIR)/TableLocator.o: $(BUILD_DIR)/TableManager.h
$(DEP_DIR)/net_table.d $(BUILD_DIR)/net_table.o: $(BUILD_DIR)/TableManager.h
