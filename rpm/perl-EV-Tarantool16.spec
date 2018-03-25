Name:           perl-EV-Tarantool16
Version:        0.1
Release:        1%{?dist}
Summary:        EV::Tarantool16
License:        GPL+
Group:          Development/Libraries
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      x86_64
BuildRequires:  perl >= 0:5.006
BuildRequires:  tarantool >= 1.6.8.0
BuildRequires:  tarantool-devel >= 1.6.8.0
BuildRequires:  c-ares >= 1.10
BuildRequires:  c-ares-devel >= 1.10
BuildRequires:  libev-devel
BuildRequires:  perl(ExtUtils::MakeMaker)
BuildRequires:  perl(EV)
BuildRequires:  perl(Types::Serialiser)
BuildRequires:  perl(Test::More)
BuildRequires:  perl(Test::Deep)
BuildRequires:  perl(AnyEvent)
BuildRequires:  perl(Proc::ProcessTable)
BuildRequires:  perl(Time::HiRes)
BuildRequires:  perl(Scalar::Util)
BuildRequires:  perl(Data::Dumper)
BuildRequires:  perl(Carp)
BuildRequires:  perl(constant)
Requires:       perl(:MODULE_COMPAT_%(eval "`%{__perl} -V:version`"; echo $version))
Requires:       tarantool >= 1.6.8.0
Requires:       c-ares >= 1.10
Requires:       perl(EV)
Requires:       perl(Types::Serialiser)

URL: https://github.com/igorcoding/EV-Tarantool16
Source0: https://github.com/igorcoding/EV-Tarantool16/archive/%{version}/EV-Tarantool16-%{version}.tar.gz

%description
EV::Tarantool16 - connector for Tarantool 1.6+

%prep
%setup -q -n EV-Tarantool16-%{version}

%build
%{__perl} Makefile.PL INSTALLDIRS=vendor
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT

make pure_install PERL_INSTALL_ROOT=$RPM_BUILD_ROOT

find $RPM_BUILD_ROOT -type f -name .packlist -exec rm -f {} \;
find $RPM_BUILD_ROOT -depth -type d -exec rmdir {} 2>/dev/null \;

%{_fixperms} $RPM_BUILD_ROOT/*

%check
TEST_FAST_MEM=1 make test

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc META.json
%{perl_vendorarch}/auto/*
%{perl_vendorarch}/EV*
%{_mandir}/man3/*

%changelog
* Sun Mar 25 2018 igorcoding 1.39-1
- Create spec
