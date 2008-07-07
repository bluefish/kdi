#------------------------------------------------------ -*- Mode: Makefile -*-
# Copyright (C) 2007 Josh Taylor (Kosmix Corporation)
# Created 2007-01-26
#
# This file is part of the Magic build system.
# 
# The Magic build system is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2 of
# the License, or any later version.
# 
# The Magic build system is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#----------------------------------------------------------------------------

#----------------------------------------------------------------------------
# Begin magic template section
#----------------------------------------------------------------------------


#----------------------------------------------------------------------------
# Define library name for this module
#----------------------------------------------------------------------------
_LIBNAME := $(subst /,_,$(MODULE))


#----------------------------------------------------------------------------
# Choose link style
#----------------------------------------------------------------------------
ifeq ($(strip $(MAGIC_LINK_TYPE)),shared)
# Shared libraries
_LINKTYPE := SO
else
ifeq ($(strip $(MAGIC_LINK_TYPE)),static)
# Static libraries
_LINKTYPE := A
else
ifeq ($(strip $(MAGIC_LINK_TYPE)),)
# Default to static libraries
_LINKTYPE := A
else
# Unknown link type
$(warning Unknown MAGIC_LINK_TYPE '$(MAGIC_LINK_TYPE)')
_LINKTYPE := A
endif
endif
endif


#----------------------------------------------------------------------------
# Split objects into different classes
#----------------------------------------------------------------------------
_MAIN_OBJ := $(filter %_main.o,$(OBJ))
_TEST_OBJ := $(filter %_test.o,$(OBJ))
_UNIT_OBJ := $(filter %_unittest.o,$(OBJ))
_PLUGIN_OBJ := $(filter %_plugin.o,$(OBJ))
_LIB_OBJ  := $(filter-out %_main.o %_test.o %_unittest.o %_plugin.o,$(OBJ))


#----------------------------------------------------------------------------
# Dependency closure rules
#----------------------------------------------------------------------------
define _DEP_UNIQ
_LAST := $(if $(strip $(1)),$(word $(words $(1)),$(1)))
#$$(warning UNIQ $(1) : $(_UNIQ) : $$(_LAST))
_UNIQ := $$(_LAST) $(_UNIQ)
$(if $(1),$$(eval $$(call _DEP_UNIQ,$$(filter-out $$(_LAST),$(1)))))
endef

define _DEP_UPDATE
KLIB_$(_LIBNAME)_MDEPS := $$(KLIB_$(_LIBNAME)_MDEPS) $$(KLIB_$(1)_MDEPS)
KLIB_$(_LIBNAME)_XDEPS := $$(KLIB_$(_LIBNAME)_XDEPS) $$(KLIB_$(1)_XDEPS)
KLIB_$(_LIBNAME)_FDEPS_CPPFLAGS := $$(KLIB_$(_LIBNAME)_FDEPS_CPPFLAGS) $$(KLIB_$(1)_FDEPS_CPPFLAGS)
KLIB_$(_LIBNAME)_FDEPS_CXXFLAGS := $$(KLIB_$(_LIBNAME)_FDEPS_CXXFLAGS) $$(KLIB_$(1)_FDEPS_CXXFLAGS)
KLIB_$(_LIBNAME)_FDEPS_CFLAGS := $$(KLIB_$(_LIBNAME)_FDEPS_CFLAGS) $$(KLIB_$(1)_FDEPS_CFLAGS)
endef

define _DEP_CLOSURE
KLIB_$(_LIBNAME)_MDEPS := $(MAGIC_MODULE_DEPS)
KLIB_$(_LIBNAME)_XDEPS := $(MAGIC_EXTERNAL_DEPS)
KLIB_$(_LIBNAME)_FDEPS_CPPFLAGS := $(MAGIC_FLAGS_CPPFLAGS)
KLIB_$(_LIBNAME)_FDEPS_CXXFLAGS := $(MAGIC_FLAGS_CXXFLAGS)
KLIB_$(_LIBNAME)_FDEPS_CFLAGS := $(MAGIC_FLAGS_CFLAGS)
$$(foreach dep,$(MAGIC_MODULE_DEPS),$$(eval $$(call _DEP_UPDATE,$$(dep))))

_UNIQ :=
$$(eval $$(call _DEP_UNIQ,$$(KLIB_$(_LIBNAME)_MDEPS)))
_MDEPS := $$(strip $$(_UNIQ))
KLIB_$(_LIBNAME)_MDEPS := $$(_MDEPS)

_UNIQ :=
$$(eval $$(call _DEP_UNIQ,$$(KLIB_$(_LIBNAME)_XDEPS)))
_XDEPS := $$(strip $$(_UNIQ))
KLIB_$(_LIBNAME)_XDEPS := $$(_XDEPS)

_UNIQ :=
$$(eval $$(call _DEP_UNIQ,$$(KLIB_$(_LIBNAME)_FDEPS_CPPFLAGS)))
_FDEPS_CPPFLAGS := $$(strip $$(_UNIQ))
KLIB_$(_LIBNAME)_FDEPS_CPPFLAGS := $$(_FDEPS_CPPFLAGS)

_UNIQ :=
$$(eval $$(call _DEP_UNIQ,$$(KLIB_$(_LIBNAME)_FDEPS_CXXFLAGS)))
_FDEPS_CXXFLAGS := $$(strip $$(_UNIQ))
KLIB_$(_LIBNAME)_FDEPS_CXXFLAGS := $$(_FDEPS_CXXFLAGS)

_UNIQ :=
$$(eval $$(call _DEP_UNIQ,$$(KLIB_$(_LIBNAME)_FDEPS_CFLAGS)))
_FDEPS_CFLAGS := $$(strip $$(_UNIQ))
KLIB_$(_LIBNAME)_FDEPS_CFLAGS := $$(_FDEPS_CFLAGS)
endef


#----------------------------------------------------------------------------
# Compute transitive closure of module dependencies
#----------------------------------------------------------------------------
$(eval $(call _DEP_CLOSURE))


#----------------------------------------------------------------------------
# Make install target closures
#----------------------------------------------------------------------------
define _INSTALL_CLOSURE
$(MODULE)_install: $(1)_install_closure
$(MODULE)_syminstall: $(1)_syminstall_closure
endef

$(_LIBNAME)_install_closure: $(MODULE)_install
	@true
$(_LIBNAME)_syminstall_closure: $(MODULE)_syminstall
	@true

$(foreach x,$(_MDEPS),$(eval $(call _INSTALL_CLOSURE,$(x))))


#----------------------------------------------------------------------------
# Add external dependency template
#----------------------------------------------------------------------------
define _ADD_EXTERNAL_DEP
$(1): -l$(2)
endef


#----------------------------------------------------------------------------
# Add module dependency template
#----------------------------------------------------------------------------
define _ADD_MODULE_DEP
KLIB_$(2)_A ?= $$(warning Undefined module '$(2)' referenced from '$(MODULE)')
KLIB_$(2)_SO ?= $$(warning Undefined module '$(2)' referenced from '$(MODULE)')
$(1): $$(KLIB_$(2)_$(_LINKTYPE))
endef


#----------------------------------------------------------------------------
# Add flag dependency template
#----------------------------------------------------------------------------
define _ADD_FLAG_DEP
$(OBJ) $(OBJ:$(build)/%.o=$(dep)/%.d): $(1) := $$($(1)) $$(_FDEPS_$(1))
endef

$(if $(_FDEPS_CPPFLAGS),$(eval $(call _ADD_FLAG_DEP,CPPFLAGS)))
$(if $(_FDEPS_CXXFLAGS),$(eval $(call _ADD_FLAG_DEP,CXXFLAGS)))
$(if $(_FDEPS_CFLAGS),$(eval $(call _ADD_FLAG_DEP,CFLAGS)))


#----------------------------------------------------------------------------
# Library template
#----------------------------------------------------------------------------
define _LIB_TEMPLATE

# Define local library names
_LIB_A := $(BUILD_DIR)/lib$(_LIBNAME).a
_LIB_SO := $(BUILD_DIR)/lib$(_LIBNAME).so

# Define global library names
KLIB_$(_LIBNAME)_A := $$(_LIB_A)
KLIB_$(_LIBNAME)_SO := $$(_LIB_SO)

# Add library objects
$$(_LIB_A) $$(_LIB_SO): $(_LIB_OBJ)

# Add dependencies on other libraries to the shared library
$$(foreach y,$(_MDEPS),$$(eval $$(call _ADD_MODULE_DEP,$$(_LIB_SO),$$(y))))
$$(foreach y,$(_XDEPS),$$(eval $$(call _ADD_EXTERNAL_DEP,$$(_LIB_SO),$$(y))))

endef


#----------------------------------------------------------------------------
# Empty library template
#----------------------------------------------------------------------------
define _LIB_EMPTY
_LIB_A :=
_LIB_SO :=
KLIB_$(_LIBNAME)_A = $$(warning Empty module '$(_LIBNAME)' referenced from '$$(MODULE)')
KLIB_$(_LIBNAME)_SO = $$(warning Empty module '$(_LIBNAME)' referenced from '$$(MODULE)')
endef


#----------------------------------------------------------------------------
# Define library targets
#----------------------------------------------------------------------------
$(if $(_LIB_OBJ), $(eval $(_LIB_TEMPLATE)), $(eval $(_LIB_EMPTY)))
_LIB_TARGETS := $(_LIB_A) $(_LIB_SO)


#----------------------------------------------------------------------------
# Application template
#----------------------------------------------------------------------------
define _APP_TEMPLATE
$(1): $(2).o
$$(if $(_LIB_OBJ),$$(eval $$(call _ADD_MODULE_DEP,$(1),$(_LIBNAME))))
$$(foreach y,$(_MDEPS),$$(eval $$(call _ADD_MODULE_DEP,$(1),$$(y))))
$$(foreach y,$(_XDEPS),$$(eval $$(call _ADD_EXTERNAL_DEP,$(1),$$(y))))
endef


#----------------------------------------------------------------------------
# Additional unittest template
#----------------------------------------------------------------------------
define _UNIT_TEMPLATE
$(1): -l$(BOOST_UNIT_TEST_FRAMEWORK)
endef


#----------------------------------------------------------------------------
# Define main targets
#----------------------------------------------------------------------------
_MAIN_TARGETS := $(_MAIN_OBJ:%_main.o=%)
$(if $(_MAIN_TARGETS),$(foreach x,$(_MAIN_TARGETS), $(eval $(call _APP_TEMPLATE,$(x),$(x)_main))))


#----------------------------------------------------------------------------
# Define test targets
#----------------------------------------------------------------------------
_TEST_TARGETS := $(_TEST_OBJ:%.o=%)
$(if $(_TEST_TARGETS),$(foreach x,$(_TEST_TARGETS), $(eval $(call _APP_TEMPLATE,$(x),$(x)))))


#----------------------------------------------------------------------------
# Define unittest targets
#----------------------------------------------------------------------------
_UNIT_TARGETS := $(_UNIT_OBJ:%.o=%)
$(if $(_UNIT_TARGETS),$(foreach x,$(_UNIT_TARGETS), $(eval $(call _APP_TEMPLATE,$(x),$(x)))))
$(if $(_UNIT_TARGETS),$(foreach x,$(_UNIT_TARGETS), $(eval $(call _UNIT_TEMPLATE,$(x),$(x)))))


#----------------------------------------------------------------------------
# Define plugin targets
#----------------------------------------------------------------------------
_PLUGIN_TARGETS := $(_PLUGIN_OBJ:%_plugin.o=%.so)
$(if $(_PLUGIN_TARGETS),$(foreach x,$(_PLUGIN_TARGETS), $(eval $(call _APP_TEMPLATE,$(x),$(x:%.so=%_plugin)))))


#----------------------------------------------------------------------------
# Export targets
#----------------------------------------------------------------------------
TARGETS := $(_MAIN_TARGETS) $(_TEST_TARGETS) $(_UNIT_TARGETS) $(_LIB_TARGETS) $(_PLUGIN_TARGETS)
LIB_INSTALL := $(_LIB_TARGETS) $(_PLUGIN_TARGETS)
BIN_INSTALL := $(_MAIN_TARGETS)
UNITTESTS := $(_UNIT_TARGETS)


#----------------------------------------------------------------------------
# End magic template section
#----------------------------------------------------------------------------
