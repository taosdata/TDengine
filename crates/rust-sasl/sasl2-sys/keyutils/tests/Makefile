# Copyright (c) 2015 Red Hat, Inc. All rights reserved. This copyrighted material
# is made available to anyone wishing to use, modify, copy, or
# redistribute it subject to the terms and conditions of the GNU General
# Public License v.2.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
#
# Author: David Howells <dhowells@redhat.com>
# Updated to new Make model by Bill Peck <bpeck@redhat.com>

# The toplevel namespace within which the test lives.
TOPLEVEL_NAMESPACE=/kernel

# The name of the package under test:
PACKAGE_NAME=keyutils

# The path of the test below the package:
RELATIVE_PATH=testsuite

# Version of the Test. Used with make tag.
export TESTVERSION=1.3

# The combined namespace of the test.
export TEST=$(TOPLEVEL_NAMESPACE)/$(PACKAGE_NAME)/$(RELATIVE_PATH)

.PHONY: all install download clean

TESTS	= $(shell /usr/bin/find . -name runtest.sh | \
                  /bin/sed -e 's/runtest.sh//g;s/^\.\///;s/\/$$//')

BUILT_FILES= 
FILES=$(METADATA) *.sh Makefile PURPOSE bugzillas keyctl

run: $(FILES) build
	bash runtest.sh $(TESTS)

build: $(BUILT_FILES)

clean:
	rm -f *~ *.rpm $(BUILT_FILES)
	find * -name test.out -delete

# You may need to add other targets e.g. to build executables from source code
# Add them here:


# Include Common Makefile
ifneq ($(wildcard /usr/share/rhts/lib/rhts-make.include),)
include /usr/share/rhts/lib/rhts-make.include

# Generate the testinfo.desc here:
$(METADATA): Makefile
	@touch $(METADATA)
	@echo "Owner:        David Howells <dhowells@redhat.com>" > $(METADATA)
	@echo "Name:         $(TEST)" >> $(METADATA)
	@echo "Path:         $(TEST_DIR)"	>> $(METADATA)
	@echo "License:      Unknown" >> $(METADATA)
	@echo "TestVersion:  $(TESTVERSION)"	>> $(METADATA)
	@echo "Description:  testsuite to verify keyutils ">> $(METADATA)
	@echo "TestTime:     30m" >> $(METADATA)
	@echo "RunFor:       $(PACKAGE_NAME)" >> $(METADATA)
	@echo "Requires:     $(PACKAGE_NAME)" >> $(METADATA)
	@echo "Requires:     redhat-lsb-core" >> $(METADATA)
	@echo "Releases:     -RHEL2.1 -RHEL3" >> $(METADATA)
	@echo "Type:         Tier1" >> $(METADATA)
	@echo "Type:         KernelTier1" >> $(METADATA)
	@echo "Destructive:  no" >> $(METADATA)
	rhts-lint $(METADATA)

endif
