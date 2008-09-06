#------------------------------------------------------ -*- Mode: Makefile -*-
# Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
# Created 2006-05-10
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
#-----------------------------------------------------------------------------

#----------------------------------------------------------------------------
# Default target
#----------------------------------------------------------------------------
all:
	@true

#----------------------------------------------------------------------------
# Automatic module list
#----------------------------------------------------------------------------
_AUTO_MODULES := build/cc/automodules.mk
-include $(_AUTO_MODULES)
$(_AUTO_MODULES): $(shell find src/cc -name module.mk) $(MODULES:%=%/module.mk)
	@echo "Generating module dependencies"
	@[ -d $(@D) ] || mkdir -p $(@D) && \
	 ./genmodules -d MAGIC_MODULE_DEPS -m "include magic.mk" -b src/cc -o $@ $^

#----------------------------------------------------------------------------
# Build config
#----------------------------------------------------------------------------

# Default variant is release
ifndef VARIANT
VARIANT := release
endif

# Set build paths
dep := build/cc/$(VARIANT)_deps
build := build/cc/$(VARIANT)
src := src/cc
env := env

# Sneaky variable definitions
empty :=
space :=$(empty) $(empty)
colon :=:

# Set install paths
ifndef prefix
prefix := $(CURDIR)
endif
ifndef exec_prefix
exec_prefix := $(prefix)
endif
ifndef bindir
bindir := $(exec_prefix)/bin
endif
ifndef libdir
libdir := $(exec_prefix)/lib
endif
ifndef pydir
pydir := $(exec_prefix)/lib/python
endif


#----------------------------------------------------------------------------
# Environment config
#----------------------------------------------------------------------------
$(foreach x,$(wildcard $(env)/*.env),$(eval $(shell V="$(x:$(env)/%.env=%) := $$(. $(x))" && echo "$$V" || echo "WARNING: failed to evaluate $(x) -- leaving variable undefined" 1>&2)))


#----------------------------------------------------------------------------
# runTests -- unit test harness
#----------------------------------------------------------------------------
define runTests
	@mkdir -p $(build)
	@rm -f $(build)/test_report.txt
	@echo "Running unit tests:"
	@n=0; f=0; for x in $(1); do \
	   echo "----------------------------------------------------------------------" \
	      >> $(build)/test_report.txt; \
	   echo "**** Running test: $$x" >> $(build)/test_report.txt; \
	   echo -n "  $$x : "; \
	   n=$$(( $$n + 1 )); \
	   LD_LIBRARY_PATH= $$x > $(build)/test_tmp.txt 2>&1 & \
	   TEST_PID=$$!; \
	   { sleep 30 && \
             kill -9 $$TEST_PID 2>/dev/null && \
             echo "**** Test timed out after 30 seconds" \
             >>$(build)/test_tmp.txt ; } & \
	   WATCHDOG_PID=$$!; \
	   wait $$TEST_PID 2>/dev/null ; \
	   STATUS=$$?; \
           kill $$WATCHDOG_PID 2>/dev/null ; \
	   wait $$WATCHDOG_PID 2>/dev/null ; \
	   if (( $$STATUS == 0 )); then \
	      cat $(build)/test_tmp.txt >> $(build)/test_report.txt; \
	      echo "**** Test passed" >> $(build)/test_report.txt; \
	      echo "passed"; \
	   else \
	      cat $(build)/test_tmp.txt >> $(build)/test_report.txt; \
	      echo "**** Test FAILED (status=$$STATUS)" >> $(build)/test_report.txt; \
	      echo "FAILED (exit status=$$STATUS)"; \
	      echo "---- BEGIN OUTPUT ----------------------------------------------------"; \
	      cat $(build)/test_tmp.txt; \
	      echo "----- END OUTPUT -----------------------------------------------------"; \
	      f=$$(( $$f + 1 )); \
	   fi; \
	   echo >> $(build)/test_report.txt; \
	done; \
	echo "$$n test(s) executed, $$f failure(s)"; \
	(( $$n > 0 )) && echo "Report at $(CURDIR)/$(build)/test_report.txt"; \
	(( $$f == 0 ))
endef


#----------------------------------------------------------------------------
# Primary targets
#----------------------------------------------------------------------------

binclean:
	@true

bininstall:
	@true

libinstall:
	@true

pyinstall:
	@true

binsyminstall:
	@true

libsyminstall:
	@true

pysyminstall:
	@true

unittest:
	$(call runTests,$^)

install: bininstall libinstall pyinstall
syminstall: binsyminstall libsyminstall pysyminstall

%.svg: %.dot
	@echo "Generating graph: $@"
	@dot -Tsvg -o $@ $^

%.png: %.dot
	@echo "Generating graph: $@"
	@dot -Tpng -o $@ $^

build/cc/moduledeps.dot: $(MODULES:%=%/module.mk)
	@echo "Collecting inter-module dependencies: $@"
	@./genmodules -d MAGIC_MODULE_DEPS -m "include magic.mk" -b src/cc -g -o $@ $^

# build/cc/externaldeps.dot: $(MODULES:%=%/module.mk)
# 	@echo "Collecting external dependencies: $@"
# 	@./genmodules -d MAGIC_EXTERNAL_DEPS -m "include magic.mk" -b src/cc -g -o $@ $^

#depgraph: build/cc/moduledeps.svg build/cc/externaldeps.svg
depgraph: build/cc/moduledeps.svg build/cc/moduledeps.png
	@true

.PHONY: all binclean clean install syminstall bininstall libinstall \
	binsyminstall libsyminstall unittest depgraph pyinstall pysyminstall


#----------------------------------------------------------------------------
# TAGS
#----------------------------------------------------------------------------
src/cc/TAGS:
	@echo "Building tags: $@"
	@find $(CURDIR)/$(@D) -type f -name \*.cc -o -name \*.cpp -o -name \*.c -o -name \*.h -o -name \*.java | etags --members -o $@ -

src/java/TAGS:
	@echo "Building tags: $@"
	@find $(CURDIR)/$(@D) -type f -name \*.cc -o -name \*.cpp -o -name \*.c -o -name \*.h -o -name \*.java | etags --members -o $@ -

src/TAGS:
	@echo "Building tags: $@"
	@find $(CURDIR)/$(@D) -type f -name \*.cc -o -name \*.cpp -o -name \*.c -o -name \*.h -o -name \*.java | etags --members -o $@ -

TAGS: src/TAGS src/cc/TAGS src/java/TAGS

.PHONY: TAGS src/TAGS src/cc/TAGS src/java/TAGS



#----------------------------------------------------------------------------
# JNI cruft
#----------------------------------------------------------------------------

# Set JNI flags
ifdef JDK_DIR
JNI_LDFLAGS := -L$(JDK_DIR)/jre/lib/amd64/server        \
               -L$(JDK_DIR)/jre/lib/amd64               \
               -ljvm -lzip -ljava -lverify
JNI_CPPFLAGS := -I$(JDK_DIR)/include -I$(JDK_DIR)/include/linux
else
JNI_LDFLAGS = $(error Need JDK_DIR)
JNI_CPPFLAGS = $(error Need JDK_DIR)
endif


#----------------------------------------------------------------------------
# Internal config
#----------------------------------------------------------------------------

# Init module accumulators
all_obj :=

# Set tool flags (with variants)
CPPFLAGS := -I$(src) -I$(build) $(CPPFLAGS)
CFLAGS := -Wall -fPIC $(CFLAGS)
CXXFLAGS := -ftemplate-depth-100 $(CXXFLAGS)
LDFLAGS := -Wl,--enable-new-dtags $(LDFLAGS)
ifeq ($(VARIANT), release)
CPPFLAGS += -DRELEASE -DNDEBUG
CFLAGS += -O3
CXXFLAGS += -finline-functions
else
ifeq ($(VARIANT), debug)
CFLAGS += -g
else
ifeq ($(VARIANT), superdebug)
#CPPFLAGS += -D_GLIBCXX_CONCEPT_CHECKS -D_GLIBCXX_DEBUG
# Having some compile/link problems with _GLIBCXX_DEBUG
CPPFLAGS += -DSUPERDEBUG -D_GLIBCXX_CONCEPT_CHECKS
CFLAGS += -ggdb3
else
ifeq ($(VARIANT), profile)
CPPFLAGS += -DPROFILE -DNDEBUG
CFLAGS += -O3 -pg
CXXFLAGS += -finline-functions
else
$(error Unknown build variant "$(VARIANT)")
endif
endif
endif
endif

ifdef LAYOUT
ifeq ($(LAYOUT), old)
CPPFLAGS += -DOLD_POSTINGS_LAYOUT
endif
endif

ifdef PLATFORM
ifeq ($(PLATFORM), spectrum)
CPPFLAGS += -DLUT_ENTRY_150
endif
endif

ifdef GRAPHSIZE
ifeq ($(GRAPHSIZE), large)
CPPFLAGS += -DGRAPH_DATATYPE_LONG
endif
endif

ifdef COMPACTGRAPHSIZE
ifeq ($(COMPACTGRAPHSIZE), large)
CPPFLAGS += -DCOMPACTGRAPH_DATATYPE_LONG
endif
endif

ifdef TARGET
ifeq ($(TARGET), qa)
CPPFLAGS += -D_TARGET_QA
endif
endif


CXXFLAGS := $(CFLAGS) $(CXXFLAGS)

ARFLAGS := crs

# Set slice vars
SLICE2CPP ?= slice2cpp
SLICEFLAGS ?= --stream --source-ext=cc -I$(SLICE_INCLUDE) $(CPPFLAGS)

#----------------------------------------------------------------------------
# Define util functions
#----------------------------------------------------------------------------

src2obj = $(patsubst $(src)/%.$(2),$(build)/%.o,$(filter %.$(2),$(1)))
srcs2obj = $(call src2obj,$(1),cc) $(call src2obj,$(1),cpp) \
           $(call src2obj,$(1),c) $(call src2obj,$(1),ice)


#----------------------------------------------------------------------------
# Define module import function
#----------------------------------------------------------------------------

define import

MODULE := $$(MNAME_$(subst /,_,$(1)))
SRC_DIR := $(1)
BUILD_DIR := $(build)/$(subst $(src)/,$(empty),$(1))
DEP_DIR := $(dep)/$(subst $(src)/,$(empty),$(1))

MAGIC_MODULE_DEPS :=
MAGIC_EXTERNAL_DEPS :=
MAGIC_FLAGS_CPPFLAGS :=
MAGIC_FLAGS_CXXFLAGS :=
MAGIC_FLAGS_CFLAGS :=
MAGIC_LINK_TYPE :=

SRC := $(wildcard $(1)/*.cc) $(wildcard $(1)/*.cpp) \
       $(wildcard $(1)/*.c) $(wildcard $(1)/*.ice)
OBJ := $$(call srcs2obj,$$(SRC))

TARGETS :=
UNITTESTS :=
BIN_INSTALL :=
LIB_INSTALL :=
PY_INSTALL :=
CLEAN_EXTRA :=

$$(warning Importing module $$(MODULE): $(1)/module.mk)
-include $(1)/module.mk

.PHONY: $$(MODULE) $$(MODULE)_clean \
	$$(MODULE)_bininstall $$(MODULE)_libinstall $$(MODULE)_pyinstall \
	$$(MODULE)_binsyminstall $$(MODULE)_libsyminstall $$(MODULE)_pysyminstall \
	$$(MODULE)_install $$(MODULE)_syminstall $$(MODULE)_unittest

$$(MODULE): $$(TARGETS)

#mod_$(subst /,_,$(1))_clean := $$(filter-out $$(BUILD_DIR)/%,$$(OBJ) $$(TARGETS) $$(CLEAN_EXTRA))
mod_$(subst /,_,$(1))_clean := $$(OBJ) $$(TARGETS) $$(CLEAN_EXTRA)

$$(MODULE)_clean:
	@echo "Cleaning $(1)"
	@rm -f $$(mod_$(subst /,_,$(1))_clean)

all: $$(MODULE)
binclean: $$(MODULE)_clean
unittest: $$(UNITTESTS)

$$(MODULE)_bininstall: $$(BIN_INSTALL) 
	@mkdir -p $(bindir)
	@for x in $$^; do \
	    echo "Install ($(VARIANT)) $(bindir)/$$$$(basename $$$$x)"; \
	    install $$$$x $(bindir)/$$$$(basename $$$$x); \
	done

$$(MODULE)_binsyminstall: $$(BIN_INSTALL) 
	@mkdir -p $(bindir)
	@for x in $$^; do \
	    echo "Symlink install ($(VARIANT)) $(bindir)/$$$$(basename $$$$x)"; \
	    ln -nfs ../$$$$x $(bindir)/$$$$(basename $$$$x); \
	done

$$(MODULE)_libinstall: $$(LIB_INSTALL) 
	@mkdir -p $(libdir)
	@for x in $$^; do \
	    echo "Install ($(VARIANT)) $(libdir)/$$$$(basename $$$$x)"; \
	    install $$$$x $(libdir)/$$$$(basename $$$$x); \
	done

$$(MODULE)_libsyminstall: $$(LIB_INSTALL) 
	@mkdir -p $(libdir)
	@for x in $$^; do \
	    echo "Symlink install ($(VARIANT)) $(libdir)/$$$$(basename $$$$x)"; \
	    ln -nfs ../$$$$x $(libdir)/$$$$(basename $$$$x); \
	done

$$(MODULE)_pyinstall: $$(PY_INSTALL) 
	@mkdir -p $(pydir)
	@for x in $$^; do \
	    echo "Install ($(VARIANT)) $(pydir)/$$$$(basename $$$$x)"; \
	    install $$$$x $(pydir)/$$$$(basename $$$$x); \
	done

$$(MODULE)_pysyminstall: $$(PY_INSTALL) 
	@mkdir -p $(pydir)
	@for x in $$^; do \
	    echo "Symlink install ($(VARIANT)) $(pydir)/$$$$(basename $$$$x)"; \
	    ln -nfs ../../$$$$x $(pydir)/$$$$(basename $$$$x); \
	done

$$(MODULE)_install: $$(MODULE)_bininstall $$(MODULE)_libinstall $$(MODULE)_pyinstall
	@true

$$(MODULE)_syminstall: $$(MODULE)_binsyminstall $$(MODULE)_libsyminstall $$(MODULE)_pysyminstall
	@true

bininstall: $$(MODULE)_bininstall
libinstall: $$(MODULE)_libinstall
pyinstall: $$(MODULE)_pyinstall
binsyminstall: $$(MODULE)_binsyminstall
libsyminstall: $$(MODULE)_libsyminstall
pysyminstall: $$(MODULE)_pysyminstall

$$(MODULE)_unittest: $$(UNITTESTS)
	$$(call runTests,$$^)

all_obj += $$(OBJ)

endef


#----------------------------------------------------------------------------
# Import modules
#----------------------------------------------------------------------------

$(foreach x,$(MODULES),$(eval $(call import,$(x))))


#----------------------------------------------------------------------------
# Invalidate import-only variables
#----------------------------------------------------------------------------

MODULE = $(error Invalid reference to MODULE variable)
SRC_DIR = $(error Invalid reference to SRC_DIR variable)
BUILD_DIR = $(error Invalid reference to BUILD_DIR variable)
DEP_DIR = $(error Invalid reference to DEP_DIR variable)
SRC = $(error Invalid reference to SRC variable)
OBJ = $(error Invalid reference to OBJ variable)
TARGETS = $(error Invalid reference to TARGETS variable)
UNITTESTS = $(error Invalid reference to UNITTESTS variable)
BIN_INSTALL = $(error Invalid reference to BIN_INSTALL variable)
LIB_INSTALL = $(error Invalid reference to LIB_INSTALL variable)
CLEAN_EXTRA = $(error Invalid reference to CLEAN_EXTRA variable)

MAGIC_MODULE_DEPS :=
MAGIC_EXTERNAL_DEPS :=
MAGIC_FLAGS_CPPFLAGS :=
MAGIC_FLAGS_CXXFLAGS :=
MAGIC_FLAGS_CFLAGS :=
MAGIC_LINK_TYPE :=

#----------------------------------------------------------------------------
# Library vpath config
#----------------------------------------------------------------------------
$(foreach x,$(filter -L%,$(LDFLAGS)),$(eval vpath %.a $(x:-L%=%)))
$(foreach x,$(filter -L%,$(LDFLAGS)),$(eval vpath %.so $(x:-L%=%)))
LDFLAGS := $(filter-out -L%,$(LDFLAGS))


#----------------------------------------------------------------------------
# Include dependencies
#----------------------------------------------------------------------------

all_dep := $(all_obj:$(build)%.o=$(dep)%.d)

.PHONY: depclean clean
depclean:
	@echo "Cleaning dependencies"
	@rm -rf $(dep)
	@rm -f $(_AUTO_MODULES)

clean: depclean binclean

ifneq ($(MAKECMDGOALS),binclean)
ifneq ($(MAKECMDGOALS),depclean)
ifneq ($(MAKECMDGOALS),clean)
ifneq ($(MAKECMDGOALS),TAGS)
ifdef all_dep
-include $(all_dep)
endif
endif
endif
endif
endif


#----------------------------------------------------------------------------
# Build rules
#----------------------------------------------------------------------------

MK_TDIR = [ -d $(@D) ] || mkdir -p $(@D)

MK_STDDIRS := /lib /lib64 /usr/lib /usr/lib64
MK_GET_DIRS = $(filter-out $(MK_STDDIRS),$(sort $(patsubst %/,%,$(dir $(1)))))
MK_GET_ABS_DIRS = $(patsubst $(build)/%,$(CURDIR)/$(build)/%,$(call MK_GET_DIRS,$(1)))

MK_RUNPATH = LD_RUN_PATH="$(subst $(space),$(colon),$(call MK_GET_ABS_DIRS,$(filter %.so,$^)))"
MK_OBJECTS = $(filter-out %.so,$+)
MK_LIBPATH = $(patsubst %,-L%,$(call MK_GET_DIRS,$(filter %.so,$^)))
MK_LIBRARIES = $(patsubst lib%,-l%,$(basename $(notdir $(filter %.so,$^))))


#----------------------------------------------------------------------------
# Compile and dependency rules for C++ (.cc extension)
#----------------------------------------------------------------------------
$(dep)/%.d: $(src)/%.cc
	@echo "Dependency scan $<"
	@$(MK_TDIR) && $(CXX) $(CPPFLAGS) $(CXXFLAGS) -MT '$(dep)/$*.d $(build)/$*.o' -MM -MF $@ $< || rm -f $@

$(build)/%.o: $(src)/%.cc
	@echo "Compile ($(VARIANT)) $<"
	@$(MK_TDIR) && $(CXX) $(CPPFLAGS) $(CXXFLAGS) -c -o $@ $<

$(build)/%.o: $(build)/%.cc
	@echo "Compile ($(VARIANT)) $<"
	@$(MK_TDIR) && $(CXX) $(CPPFLAGS) -I$(<D) $(CXXFLAGS) -c -o $@ $<


#----------------------------------------------------------------------------
# Compile and dependency rules for C++ (.cpp extension)
#----------------------------------------------------------------------------
$(dep)/%.d: $(src)/%.cpp
	@echo "Dependency scan $<"
	@$(MK_TDIR) && $(CXX) $(CPPFLAGS) $(CXXFLAGS) -MT '$(dep)/$*.d $(build)/$*.o' -MM -MF $@ $< || rm -f $@

$(build)/%.o: $(src)/%.cpp
	@echo "Compile ($(VARIANT)) $<"
	@$(MK_TDIR) && $(CXX) $(CPPFLAGS) $(CXXFLAGS) -c -o $@ $<

$(build)/%.o: $(build)/%.cpp
	@echo "Compile ($(VARIANT)) $<"
	@$(MK_TDIR) && $(CXX) $(CPPFLAGS) -I$(<D) $(CXXFLAGS) -c -o $@ $<


#----------------------------------------------------------------------------
# Compile and dependency rules for C
#----------------------------------------------------------------------------
$(dep)/%.d: $(src)/%.c
	@echo "Dependency scan $<"
	@$(MK_TDIR) && $(CC) $(CPPFLAGS) $(CFLAGS) -MT '$(dep)/$*.d $(build)/$*.o' -MM -MF $@ $< || rm -f $@

$(build)/%.o: $(src)/%.c
	@echo "Compile ($(VARIANT)) $<"
	@$(MK_TDIR) && $(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

$(build)/%.o: $(build)/%.c
	@echo "Compile ($(VARIANT)) $<"
	@$(MK_TDIR) && $(CC) $(CPPFLAGS) -I$(<D) $(CFLAGS) -c -o $@ $<


#----------------------------------------------------------------------------
# Slice to C++ translation
#----------------------------------------------------------------------------
$(dep)/%.d: $(src)/%.ice
	@echo "Dependency scan $<"
	@$(MK_TDIR) && $(SLICE2CPP) $(SLICEFLAGS) --depend $< | sed 's!$(*F).cpp!$(dep)/$*.d $(build)/$*.cc $(build)/$*.h!' > $@ || rm -f $@

$(build)/%.h $(build)/%.cc: $(src)/%.ice
	@echo "Slice ($(VARIANT)) $<"
	@$(MK_TDIR) && $(SLICE2CPP) $(SLICEFLAGS) --output-dir=$(@D) $<


#----------------------------------------------------------------------------
# Wrapper generator for Java SWIG files
#----------------------------------------------------------------------------
$(build)/%_wrap_java.cc: $(src)/%.i
	@echo "Swig (Java) $<"
	@$(MK_TDIR) && swig -java -c++ $(SWIGFLAGS) -o $@ $<


#----------------------------------------------------------------------------
# Archive rule for static libraries
#----------------------------------------------------------------------------
%.a:
	@echo "Archive $@"
	@rm -f $@ && $(MK_TDIR) && $(AR) $(ARFLAGS) $@ $^


#----------------------------------------------------------------------------
# Rule for shared libraries
#----------------------------------------------------------------------------
%.so:
	@echo "Link (library) $@"
	@$(MK_TDIR) && $(MK_RUNPATH) $(CXX) -rdynamic -shared $(CXXFLAGS) -o $@ $(MK_OBJECTS) $(MK_LIBPATH) $(LDFLAGS) $(MK_LIBRARIES)

#----------------------------------------------------------------------------
# Error rule for missing module files
#----------------------------------------------------------------------------
%/module.mk:
	@echo "Missing module file: $@"

#----------------------------------------------------------------------------
# Error rules for missing source files
#----------------------------------------------------------------------------
%.cc:
	@echo "Missing source file: $@"

%.c:
	@echo "Missing source file: $@"

%.cpp:
	@echo "Missing source file: $@"

%.h:
	@echo "Missing source file: $@"

%.ice:
	@echo "Missing source file: $@"

#----------------------------------------------------------------------------
# Error rule for missing libraries
#----------------------------------------------------------------------------
-l%:
	@echo "Missing external library $@"
	@false


#----------------------------------------------------------------------------
# Linking rule for programs (this rule also gets all mistyped targets,
#   which is bad)
#----------------------------------------------------------------------------
%:
	@echo "Link $@"
	@$(MK_TDIR) && $(MK_RUNPATH) $(CXX) -rdynamic $(CXXFLAGS) -o $@ $(MK_OBJECTS) $(MK_LIBPATH) $(LDFLAGS) $(MK_LIBRARIES)


#----------------------------------------------------------------------------
# Debug rule for displaying variable values
#   make FOO.show      -->  [undefined] FOO = 
#   make CC.show       -->  [default] CC = cc
#   make MK_TDIR.show  -->  [file] MK_TDIR = [ -d $(@D) ] || mkdir -p $(@D)
#----------------------------------------------------------------------------
%.show:
	@echo '[$(origin $*)] $* = $(value $*)'

#----------------------------------------------------------------------------
# Show targets
#----------------------------------------------------------------------------
targets:
	@echo "Unified targets: all(default) install syminstall clean depclean"
	@echo "Module targets: $(MODULES)"
	@echo "Clean targets: $(MODULES:%=%_clean)"
	@echo "Help targets: targets <ANY-VARIABLE>.show"
# DO NOT DELETE
