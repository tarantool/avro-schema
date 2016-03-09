Name: tarantool-avro
Version: 1.0.0
Release: 1%{?dist}
Summary: Apache Avro bindings for Tarantool
Group: Applications/Databases
License: BSD
URL: https://github.com/tarantool/avro
Source0: https://github.com/tarantool/%{name}/archive/%{version}/%{name}-%{version}.tar.gz
BuildRequires: cmake >= 2.8
BuildRequires: gcc >= 4.5
BuildRequires: tarantool-devel >= 1.6.8.0
BuildRequires: jansson-devel >= 2.0.0
BuildRequires: /usr/bin/prove
Requires: tarantool >= 1.6.8.0
Requires: jansson >= 2.0.0

%description
This package provides Apache Avro bindings for Tarantool.

%prep
%setup -q -n %{name}-%{version}

%build
%cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo
make %{?_smp_mflags}

%check
make %{?_smp_mflags} check

%install
%make_install

%files
%{_libdir}/tarantool/avro.so

%changelog
* Wed Mar 9 2016 Nick Zavaritsky <mejedi@tarantool.org> 1.0.0-1
- Initial version
